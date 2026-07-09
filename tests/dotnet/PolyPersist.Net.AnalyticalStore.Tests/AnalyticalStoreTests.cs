namespace PolyPersist.Net.AnalyticalStore.Tests
{
    /// <summary>
    /// Contract-level tests for every analytical (OLAP) backend, run data-driven over
    /// <see cref="TestMain.StoreInstances"/>. Covers table lifecycle, batch ingestion, and the core
    /// analytical read path: filtering, projection, ordering, scalar aggregates and GROUP BY.
    /// </summary>
    [TestClass]
    public class AnalyticalStoreTests
    {
        // EU: 30 (2 rows) | US: 80 (2 rows) | APAC: 70 (1 row) | grand total 180, 5 rows, qty 18.
        private static List<Sale> SampleSales() =>
        [
            new Sale { Region = "EU",   Product = "A", Quantity = 2, Amount = 10m, SoldAt = new DateTime(2026, 1, 1) },
            new Sale { Region = "EU",   Product = "B", Quantity = 1, Amount = 20m, SoldAt = new DateTime(2026, 1, 2) },
            new Sale { Region = "US",   Product = "A", Quantity = 5, Amount = 50m, SoldAt = new DateTime(2026, 1, 3) },
            new Sale { Region = "US",   Product = "A", Quantity = 3, Amount = 30m, SoldAt = new DateTime(2026, 1, 4) },
            new Sale { Region = "APAC", Product = "C", Quantity = 7, Amount = 70m, SoldAt = new DateTime(2026, 1, 5) },
        ];

        private static async Task<(IAnalyticalStore store, IAnalyticalTable<Sale> table, string name)> NewTable(
            Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            var name = TestMain.NewTableName();
            var table = await store.CreateTable<Sale>(name);
            return (store, table, name);
        }

        // ---- store metadata ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StorageModel_And_ProviderName(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            Assert.AreEqual(IStore.StorageModels.Analytical, store.StorageModel);
            Assert.IsFalse(string.IsNullOrWhiteSpace(store.ProviderName));
        }

        // ---- table lifecycle ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task CreateTable_Then_IsTableExists_True(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (store, table, name) = await NewTable(factory);

            Assert.IsTrue(await store.IsTableExists(name));
            Assert.AreEqual(name, table.Name);
            Assert.AreSame(store, table.ParentStore);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task CreateTable_Twice_Throws(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (store, _, name) = await NewTable(factory);
            await Assert.ThrowsExceptionAsync<Exception>(() => store.CreateTable<Sale>(name));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetTableByName_Missing_Throws(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            await Assert.ThrowsExceptionAsync<Exception>(() => store.GetTableByName<Sale>(TestMain.NewTableName()));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetTableByName_Existing_Ok(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (store, _, name) = await NewTable(factory);
            var again = await store.GetTableByName<Sale>(name);
            Assert.AreEqual(name, again.Name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DropTable_Then_IsTableExists_False(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (store, _, name) = await NewTable(factory);
            await store.DropTable(name);
            Assert.IsFalse(await store.IsTableExists(name));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DropTable_Missing_Throws(Func<string, Task<IAnalyticalStore>> factory)
        {
            var store = await factory(null!);
            await Assert.ThrowsExceptionAsync<Exception>(() => store.DropTable(TestMain.NewTableName()));
        }

        // ---- ingestion ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task InsertBatch_Then_Query_ReturnsAllRows(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            Assert.AreEqual(5, table.Query().Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task InsertBatch_Empty_NoOp(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(new List<Sale>());
            Assert.AreEqual(0, table.Query().Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task InsertBatch_Null_NoOp(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(null);
            Assert.AreEqual(0, table.Query().Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task InsertBatch_MultipleCalls_Accumulate(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            await table.InsertBatch(SampleSales());
            Assert.AreEqual(10, table.Query().Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task InsertBatch_LargeBatch(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            var rows = Enumerable.Range(0, 1000)
                .Select(i => new Sale { Region = "R" + (i % 4), Product = "P" + (i % 10), Quantity = i, Amount = i, SoldAt = new DateTime(2026, 1, 1).AddMinutes(i) })
                .ToList();
            await table.InsertBatch(rows);
            Assert.AreEqual(1000, table.Query().Count());
        }

        // ---- read: filter / project / order ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task EmptyTable_Query_ReturnsEmpty(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            Assert.IsFalse(table.Query().Any());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_Where_FiltersByDimension(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            Assert.AreEqual(2, table.Query().Where(s => s.Region == "US").Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_Where_FiltersByMeasure(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            // Quantity >= 5: US(5) + APAC(7) = 2 rows.
            Assert.AreEqual(2, table.Query().Where(s => s.Quantity >= 5).Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_OrderBy_Descending(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            var top = table.Query().OrderByDescending(s => s.Amount).First();
            Assert.AreEqual(70m, top.Amount);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_Projection_Distinct(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());
            var regions = table.Query().Select(s => s.Region).Distinct().ToList();
            CollectionAssert.AreEquivalent(new[] { "EU", "US", "APAC" }, regions);
        }

        // ---- read: aggregation (the analytical core) ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_ScalarAggregates(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());

            var q = table.Query();
            Assert.AreEqual(5, q.Count());
            Assert.AreEqual(180m, q.Sum(s => s.Amount));
            Assert.AreEqual(18, q.Sum(s => s.Quantity));
            Assert.AreEqual(70m, q.Max(s => s.Amount));
            Assert.AreEqual(10m, q.Min(s => s.Amount));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_GroupBy_SumByRegion(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            await table.InsertBatch(SampleSales());

            var byRegion = table.Query()
                .GroupBy(s => s.Region)
                .Select(g => new { Region = g.Key, Total = g.Sum(x => x.Amount), Rows = g.Count() })
                .ToList()
                .ToDictionary(x => x.Region);

            Assert.AreEqual(3, byRegion.Count);
            Assert.AreEqual(30m, byRegion["EU"].Total);
            Assert.AreEqual(2, byRegion["EU"].Rows);
            Assert.AreEqual(80m, byRegion["US"].Total);
            Assert.AreEqual(2, byRegion["US"].Rows);
            Assert.AreEqual(70m, byRegion["APAC"].Total);
            Assert.AreEqual(1, byRegion["APAC"].Rows);
        }

        // ---- escape hatch ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetUnderlyingImplementation_NotNull(Func<string, Task<IAnalyticalStore>> factory)
        {
            var (_, table, _) = await NewTable(factory);
            var underlying = table.GetUnderlyingImplementation();
            Assert.IsNotNull(underlying);
            // Do not dispose: the escape-hatch return's ownership is implementation-specific (e.g.
            // BigQuery hands back the store's shared client, which must outlive this call).
        }
    }
}
