namespace PolyPersist.Net.AnalyticalStore.Tests
{
    /// <summary>
    /// Thorough, result-based LINQ coverage for the analytical query surface, run data-driven over
    /// every backend. On PostgreSQL/ClickHouse this exercises linq2db; on BigQuery it drives the
    /// hand-built LINQ-to-GoogleSQL provider through all of its branches (every comparison and
    /// boolean operator, ordering, Take, Distinct, all aggregates, First/FirstOrDefault, full-row
    /// materialization, grouped Min/Max/Avg, and all column types). Because assertions are on
    /// results, identical behaviour is verified across providers.
    /// </summary>
    [TestClass]
    public class AnalyticalQueryTests
    {
        private static readonly Guid G1 = new("11111111-1111-1111-1111-111111111111");
        private static readonly Guid G2 = new("22222222-2222-2222-2222-222222222222");
        private static readonly Guid G3 = new("33333333-3333-3333-3333-333333333333");

        // Amounts 10,20,50,30,70 | Quantities 2,1,5,3,7 | Regions EU,EU,US,US,APAC
        private static List<Sale> Sales() =>
        [
            new Sale { Region = "EU",   Product = "A", Quantity = 2, Amount = 10m, SoldAt = new DateTime(2026, 1, 1) },
            new Sale { Region = "EU",   Product = "B", Quantity = 1, Amount = 20m, SoldAt = new DateTime(2026, 1, 2) },
            new Sale { Region = "US",   Product = "A", Quantity = 5, Amount = 50m, SoldAt = new DateTime(2026, 1, 3) },
            new Sale { Region = "US",   Product = "A", Quantity = 3, Amount = 30m, SoldAt = new DateTime(2026, 1, 4) },
            new Sale { Region = "APAC", Product = "C", Quantity = 7, Amount = 70m, SoldAt = new DateTime(2026, 1, 5) },
        ];

        private static List<Metric> Metrics() =>
        [
            new Metric { Name = "a", Flag = true,  Ratio = 1.5, Big = 100, Score = 10,   Ref = G1, At = new DateTime(2026, 2, 1, 10, 0, 0) },
            new Metric { Name = "b", Flag = false, Ratio = 2.5, Big = 200, Score = null, Ref = G2, At = new DateTime(2026, 2, 2, 11, 0, 0) },
            new Metric { Name = "c", Flag = true,  Ratio = 4.0, Big = 300, Score = 30,   Ref = G3, At = new DateTime(2026, 2, 3, 12, 0, 0) },
        ];

        private static async Task<IAnalyticalTable<Sale>> WithSales(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            var table = await store.CreateTable<Sale>(TestMain.NewTableName());
            await table.InsertBatch(Sales());
            return table;
        }

        private static async Task<IAnalyticalTable<Metric>> WithMetrics(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            var table = await store.CreateTable<Metric>(TestMain.NewTableName());
            await table.InsertBatch(Metrics());
            return table;
        }

        // ---- comparison operators ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_Equal(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Region == "US").Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_NotEqual(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(3, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Region != "US").Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_GreaterThan(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Amount > 30m).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_GreaterThanOrEqual(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(3, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Amount >= 30m).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_LessThan(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Amount < 30m).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_LessThanOrEqual(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(3, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Amount <= 30m).Count());

        // ---- boolean combinators ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_And(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(1, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Region == "EU" && s.Amount >= 20m).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Where_Or(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Region == "APAC" || s.Amount >= 50m).Count());

        // ---- ordering / paging ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task OrderBy_Ascending_First(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(10m, (await WithSales(factory)).QueryCrossPartition().OrderBy(s => s.Amount).First().Amount);

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task OrderBy_Take(Func<string, Task<IAnalyticalStore>> factory)
        {
            var top2 = (await WithSales(factory)).QueryCrossPartition().OrderBy(s => s.Amount).Take(2).ToList();
            CollectionAssert.AreEqual(new[] { 10m, 20m }, top2.Select(s => s.Amount).ToArray());
        }

        // ---- terminals ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Aggregate_Average(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(36m, (await WithSales(factory)).QueryCrossPartition().Average(s => s.Amount));

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Sum_With_Where(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(30m, (await WithSales(factory)).QueryCrossPartition().Where(s => s.Region == "EU").Sum(s => s.Amount));

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task FirstOrDefault_Empty_ReturnsNull(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            var table = await store.CreateTable<Sale>(TestMain.NewTableName());
            Assert.IsNull(table.QueryCrossPartition().FirstOrDefault());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task FullRows_ToList_Materializes(Func<string, Task<IAnalyticalStore>> factory)
        {
            var all = (await WithSales(factory)).QueryCrossPartition().ToList();
            Assert.AreEqual(5, all.Count);
            Assert.AreEqual(180m, all.Sum(s => s.Amount));
            var us = all.Where(s => s.Region == "US").OrderBy(s => s.Amount).ToList();
            Assert.AreEqual("A", us[0].Product);
            Assert.AreEqual(new DateTime(2026, 1, 4), us[0].SoldAt);
        }

        // ---- grouped Min/Max/Average projection ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GroupBy_MinMaxAvg(Func<string, Task<IAnalyticalStore>> factory)
        {
            var byRegion = (await WithSales(factory)).QueryCrossPartition()
                .GroupBy(s => s.Region)
                .Select(g => new { Region = g.Key, Lo = g.Min(x => x.Amount), Hi = g.Max(x => x.Amount), Avg = g.Average(x => x.Amount) })
                .ToList()
                .ToDictionary(x => x.Region);

            Assert.AreEqual(30m, byRegion["US"].Lo);
            Assert.AreEqual(50m, byRegion["US"].Hi);
            Assert.AreEqual(40m, byRegion["US"].Avg);
            Assert.AreEqual(70m, byRegion["APAC"].Lo);
        }

        // ---- column types (bool / double / long / nullable / Guid / DateTime) ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Metric_Roundtrip_AllTypes(Func<string, Task<IAnalyticalStore>> factory)
        {
            var rows = (await WithMetrics(factory)).QueryCrossPartition().ToList().OrderBy(m => m.Big).ToList();
            Assert.AreEqual(3, rows.Count);

            var b = rows[1]; // Name "b"
            Assert.AreEqual("b", b.Name);
            Assert.IsFalse(b.Flag);
            Assert.AreEqual(2.5, b.Ratio, 0.0001);
            Assert.AreEqual(200L, b.Big);
            Assert.IsNull(b.Score);            // nullable NULL round-trip
            Assert.AreEqual(G2, b.Ref);        // Guid round-trip
            Assert.AreEqual(new DateTime(2026, 2, 2, 11, 0, 0), b.At);

            Assert.AreEqual(10, rows[0].Score);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Metric_Where_Bool(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithMetrics(factory)).QueryCrossPartition().Where(m => m.Flag).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Metric_Where_Long(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(2, (await WithMetrics(factory)).QueryCrossPartition().Where(m => m.Big >= 200L).Count());

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Metric_Sum_Double(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(8.0, (await WithMetrics(factory)).QueryCrossPartition().Sum(m => m.Ratio), 0.0001);

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task FullRows_Distinct(Func<string, Task<IAnalyticalStore>> factory)
            => Assert.AreEqual(5, (await WithSales(factory)).QueryCrossPartition().Distinct().ToList().Count);
    }
}
