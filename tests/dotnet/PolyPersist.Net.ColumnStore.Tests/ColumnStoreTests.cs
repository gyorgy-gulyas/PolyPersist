using Cassandra;
using PolyPersist.Net.Attributes;
using PolyPersist.Net.Core;
using PolyPersist.Net.Test;
using System.Reflection;
using System.Security.Principal;


namespace PolyPersist.Net.ColumnStore.Tests
{
    #region
    public class SampleRow : Entity, IRow
    {
        [ClusteringColumn(1)]
        public string str_value { get; set; } = "";
        [ClusteringColumn(2)]
        public int int_value { get; set; }
        public decimal decimal_value { get; set; }
        public double double_value { get; set; }
        public float float_value { get; set; }
        public bool bool_value { get; set; }
        public DateOnly date_value { get; set; }
        public TimeOnly time_value { get; set; }
        public DateTime datetime_value { get; set; }
        public enum EnumValues
        {
            One,
            Two,
            Three
        }
        public EnumValues enum_value { get; set; }
    }
    #endregion

    [TestClass]
    public class ColumnStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_BasicInfo_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.ColumnStore, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }


        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Table_Ok(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Assert.IsFalse(await store.IsTableExists("sampletable"));

            var table = await store.CreateTable<SampleRow>("sampletable");
            Assert.IsTrue(await store.IsTableExists("sampletable"));
            Assert.AreEqual("sampletable", table.Name);
            Assert.IsNotNull(table.GetUnderlyingImplementation());

            var get = await store.GetTableByName<SampleRow>("sampletable");
            Assert.IsNotNull(get);
            Assert.AreEqual("sampletable", get.Name);

            await store.DropTable("sampletable");
            Assert.IsFalse(await store.IsTableExists("sampletable"));
        }

        public partial class Account
        {
            public enum Types
            {
                User,
                ExternalSystem,
                InternalService,
            }
        }

        public partial class Auth
        {
            public enum Methods
            {
                Email,
                ActiveDirectory,
                KAU,
                Certificate,
            }
        }

        public partial class LoginAuditEventLog
        {
            #region IBaseEntity
            public string id { get; set; } = null!;
            public string etag { get; set; } = null!;
            public DateTime LastUpdate { get; set; }
            #endregion IBaseEntity

            #region IAuditLog
            public string operation { get; set; } = null!;
            [ClusteringColumn(1)]
            public DateTime timestamp { get; set; }
            public string idenityId { get; set; } = null!;
            public string idenityName { get; set; } = null!;
            public string correlationId { get; set; } = null!;
            public string payload { get; set; } = null!;
            #endregion IAuditLog

            public Account.Types AccountType { get; set; }
            public Auth.Methods authMethod { get; set; }
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Table_Complex_Ok(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var tableName = typeof(LoginAuditEventLog).Name;

            Assert.IsFalse(await store.IsTableExists(tableName));

            var table = await store.CreateTable<SampleRow>(tableName);
            Assert.IsTrue(await store.IsTableExists(tableName));
            Assert.AreEqual(tableName, table.Name);
            Assert.IsNotNull(table.GetUnderlyingImplementation());

            var get = await store.GetTableByName<SampleRow>(tableName);
            Assert.IsNotNull(get);
            Assert.AreEqual(tableName, get.Name);

            await store.DropTable(tableName);
            Assert.IsFalse(await store.IsTableExists(tableName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_CreateTable_Fail(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            var table = await store.CreateTable<SampleRow>("sampletable");

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.CreateTable<SampleRow>("sampletable"));
            Assert.IsTrue(ex.Message.Contains("already exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_GetTable_Fail(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.GetTableByName<SampleRow>("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_DropTable_Fail(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.DropTable("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }
    }
}
