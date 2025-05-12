using PolyPersist.Net.Test;
using System.Reflection;
using System.Text;

namespace PolyPersist.Net.BlobStore.Tests
{
    [TestClass]
    public class BlobContainerTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_BasicInfo_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();

            IBlobContainer<SampleBlob> container = await store.CreateContainer<SampleBlob>(testName);
            Assert.AreEqual(testName, container.Name);
            Assert.IsNotNull(container.GetUnderlyingImplementation());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Upload_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();

            IBlobContainer<SampleBlob> container = await store.CreateContainer<SampleBlob>(testName);

            SampleBlob sample = new()
            {
                contentType = "application/json",
                fileName = "test.txt",

                PartitionKey = "test_pk",
                bool_value = true,
                datetime_value = DateTime.UtcNow,
                date_value = DateOnly.Parse("2024.04.24"),
                decimal_value = 1.0m,
                int_value = 11,
                str_value = "test text",
                time_value = TimeOnly.Parse("14:30"),
            };

            using MemoryStream memoryStream = new MemoryStream();
            string text = "Hello, MemoryStream!";
            byte[] bytesToWrite = Encoding.UTF8.GetBytes(text);
            memoryStream.Write(bytesToWrite, 0, bytesToWrite.Length);

            await container.Upload(sample, memoryStream);
            Assert.IsFalse(string.IsNullOrEmpty(sample.id));
            Assert.IsFalse(string.IsNullOrEmpty(sample.etag));
            Assert.AreNotEqual(sample.LastUpdate, DateTime.MinValue);
            Assert.AreEqual(DateTimeKind.Utc, sample.LastUpdate.Kind);

            var uploaded = await container.Find(sample.PartitionKey, sample.id);
            Assert.AreEqual(sample.contentType, uploaded.contentType);
            Assert.AreEqual(sample.fileName, uploaded.fileName);
            Assert.AreEqual(sample.PartitionKey, uploaded.PartitionKey);
            Assert.AreEqual(sample.bool_value, uploaded.bool_value);
            Assert.AreEqual(DateTimeKind.Utc, sample.datetime_value.Kind);
            Assert.AreEqual(sample.datetime_value, uploaded.datetime_value);
            Assert.AreEqual(sample.date_value, uploaded.date_value);
            Assert.AreEqual(sample.decimal_value, uploaded.decimal_value);
            Assert.AreEqual(sample.int_value, uploaded.int_value);
            Assert.AreEqual(sample.str_value, uploaded.str_value);
            Assert.AreEqual(sample.time_value, uploaded.time_value);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Upload_Fail(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();

            IBlobContainer<SampleBlob> container = await store.CreateContainer<SampleBlob>(testName);

            using MemoryStream memoryStream = new MemoryStream();
            string text = "Hello, MemoryStream!";
            byte[] bytesToWrite = Encoding.UTF8.GetBytes(text);
            memoryStream.Write(bytesToWrite, 0, bytesToWrite.Length);

            SampleBlob sample = new()
            {
                contentType = "application/json",
                fileName = "test.txt",
                PartitionKey = "test_pk",
            };
            await container.Upload(sample, memoryStream);
        }
    }
}