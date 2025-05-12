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

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Download_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sample = new SampleBlob
            {
                contentType = "text/plain",
                fileName = "download.txt",
                PartitionKey = "pk1"
            };

            string originalText = "Download this text";
            using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes(originalText));
            await container.Upload(sample, uploadStream);

            using var downloadStream = await container.Download(sample);
            using var reader = new StreamReader(downloadStream);
            var resultText = await reader.ReadToEndAsync();

            Assert.AreEqual(originalText, resultText);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Delete_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sample = new SampleBlob
            {
                fileName = "delete_me.txt",
                PartitionKey = "pk_del"
            };

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes("delete content"));
            await container.Upload(sample, stream);
            await container.Delete(sample.PartitionKey, sample.id);

            var deleted = await container.Find(sample.PartitionKey, sample.id);
            Assert.IsNull(deleted);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_UpdateContent_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sample = new SampleBlob
            {
                fileName = "update.txt",
                PartitionKey = "pk_update"
            };

            using var originalStream = new MemoryStream(Encoding.UTF8.GetBytes("Original content"));
            await container.Upload(sample, originalStream);

            using var updateStream = new MemoryStream(Encoding.UTF8.GetBytes("Updated content"));
            await container.UpdateContent(sample, updateStream);

            using var downloaded = await container.Download(sample);
            using var reader = new StreamReader(downloaded);
            string updatedText = await reader.ReadToEndAsync();

            Assert.AreEqual("Updated content", updatedText);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_UpdateMetadata_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sample = new SampleBlob
            {
                fileName = "meta.txt",
                PartitionKey = "pk_meta",
                str_value = "initial"
            };

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes("some content"));
            await container.Upload(sample, stream);

            sample.str_value = "updated metadata";
            await container.UpdateMetadata(sample);

            var found = await container.Find(sample.PartitionKey, sample.id);
            Assert.AreEqual("updated metadata", found.str_value);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Download_NotFound_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var nonExistentBlob = new SampleBlob
            {
                id = "nonexistent-id",
                PartitionKey = "invalid-pk"
            };

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                using var stream = await container.Download(nonExistentBlob);
            });
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Delete_NotFound_OK(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.Delete("invalid-pk", "nonexistent-id");
            });
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Find_NotFound_ReturnsNull(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var found = await container.Find("invalid-pk", "nonexistent-id");
            Assert.IsNull(found);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Upload_NullStream_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var blob = new SampleBlob
            {
                PartitionKey = "test-pk",
                fileName = "test.txt"
            };

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.Upload(blob, null);
            });
            Assert.IsTrue(exception.Message.Contains("cannot be read"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_UpdateContent_NullStream_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var blob = new SampleBlob
            {
                PartitionKey = "pk_update",
                fileName = "update.txt"
            };

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes("initial content"));
            await container.Upload(blob, stream);

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.UpdateContent(blob, null);
            });
            Assert.IsTrue(exception.Message.Contains("cannot be read"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_UpdateContent_NotFound_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var fake = new SampleBlob
            {
                id = "ghost-id",
                PartitionKey = "ghost-pk",
                fileName = "nonexistent.txt"
            };

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes("new content"));

            await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.UpdateContent(fake, stream);
            });
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_UpdateMetadata_NotFound_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var fake = new SampleBlob
            {
                id = "nonexistent-id",
                PartitionKey = "nonexistent-pk",
                fileName = "ghost.txt",
                str_value = "meta"
            };

            await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.UpdateMetadata(fake);
            });
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Download_AfterDelete_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sample = new SampleBlob
            {
                PartitionKey = "pk_del_after_download",
                fileName = "to_be_deleted.txt"
            };

            using var uploadStream = new MemoryStream(Encoding.UTF8.GetBytes("temp content"));
            await container.Upload(sample, uploadStream);

            await container.Delete(sample.PartitionKey, sample.id);

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                using var stream = await container.Download(sample);
            });
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        class NonReadableStream : Stream
        {
            public override bool CanRead => false;
            public override bool CanSeek => true;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position { get; set; }
            public override void Flush() => throw new NotSupportedException();
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Upload_UnreadableStream_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var blob = new SampleBlob
            {
                PartitionKey = "pk_invalid_stream",
                fileName = "invalid.bin"
            };

            using var unreadableStream = new NonReadableStream();

            var ex = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.Upload(blob, unreadableStream);
            });
            Assert.IsTrue(ex.Message.Contains("cannot be read"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_SameId_DifferentPartitionKey_StoresIndependently(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var sharedId = Guid.NewGuid().ToString();

            var blob1 = new SampleBlob
            {
                id = sharedId,
                PartitionKey = "pk1",
                fileName = "blob1.txt"
            };
            var blob2 = new SampleBlob
            {
                id = sharedId,
                PartitionKey = "pk2",
                fileName = "blob2.txt"
            };

            using var stream1 = new MemoryStream(Encoding.UTF8.GetBytes("content1"));
            using var stream2 = new MemoryStream(Encoding.UTF8.GetBytes("content2"));

            await container.Upload(blob1, stream1);
            await container.Upload(blob2, stream2);

            var found1 = await container.Find("pk1", sharedId);
            var found2 = await container.Find("pk2", sharedId);

            Assert.IsNotNull(found1);
            Assert.IsNotNull(found2);
            Assert.AreEqual("blob1.txt", found1.fileName);
            Assert.AreEqual("blob2.txt", found2.fileName);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Upload_NullOrEmptyPartitionKey_Fails(Func<Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName();
            var store = await factory();
            var container = await store.CreateContainer<SampleBlob>(testName);

            var blob = new SampleBlob
            {
                fileName = "invalid_partition.txt"
            };

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            blob.PartitionKey = null!;
            var ex1 = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.Upload(blob, stream);
            });
            Assert.IsTrue(ex1.Message.Contains("PartitionKey must be filled"));

            blob.PartitionKey = "";
            var ex2 = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await container.Upload(blob, stream);
            });
            Assert.IsTrue(ex2.Message.Contains("PartitionKey must be filled"));
        }
    }
}