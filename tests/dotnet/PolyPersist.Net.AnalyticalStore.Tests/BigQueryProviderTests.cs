namespace PolyPersist.Net.AnalyticalStore.Tests
{
    /// <summary>
    /// BigQuery-provider-specific tests that cannot be expressed against the linq2db backends
    /// (which support the operators natively). These pin down the custom provider's guard rails:
    /// operators it deliberately does not translate throw NotSupportedException.
    /// </summary>
    [TestClass]
    public class BigQueryProviderTests
    {
        private static async Task<IAnalyticalTable<Sale>> Table()
        {
            var store = await TestMain.BigQueryFactory(null);
            var table = await store.CreateTable<Sale>(TestMain.NewTableName());
            await table.InsertBatch(
            [
                new Sale { Region = "EU", Product = "A", Quantity = 1, Amount = 10m, SoldAt = new DateTime(2026, 1, 1) },
                new Sale { Region = "US", Product = "B", Quantity = 2, Amount = 20m, SoldAt = new DateTime(2026, 1, 2) },
            ]);
            return table;
        }

        [TestMethod]
        public async Task Skip_NotSupported()
        {
            var table = await Table();
            var ex = Assert.ThrowsException<NotSupportedException>(() => table.Query().Skip(1).ToList());
            Assert.IsTrue(ex.Message.Contains("Skip"));
        }

        [TestMethod]
        public async Task StringMemberFunction_InWhere_NotSupported()
        {
            var table = await Table();
            // s.Region.StartsWith(...) is not a column/param/comparison the provider translates.
            Assert.ThrowsException<NotSupportedException>(() => table.Query().Where(s => s.Region.StartsWith("E")).ToList());
        }
    }
}
