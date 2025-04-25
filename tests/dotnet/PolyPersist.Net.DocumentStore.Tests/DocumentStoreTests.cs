using PolyPersist.Net.Core;
using PolyPersist.Net.Test;
using System.Reflection;


namespace PolyPersist.Net.DocumentStore.Tests
{
    #region
    class SampleDocument : Document
    {
        public string str_value { get; set; }
        public int int_value { get; set; }
        public decimal decimal_value { get; set; }
        public bool bool_value { get; set; }
        public DateOnly date_value { get; set; }
        public TimeOnly time_value { get; set; }
        public DateTime datetime_value { get; set; }
    }
    #endregion

    [TestClass]
    public class DocumentStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_BasicInfo_OK(Func<string,Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.Document, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }
    }
}