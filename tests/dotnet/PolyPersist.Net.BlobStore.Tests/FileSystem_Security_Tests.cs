using Microsoft.VisualStudio.TestTools.UnitTesting;
using PolyPersist;
using PolyPersist.Net.BlobStore.FileSystem;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.Tests
{
    // PP-02 (traversal) + PP-18 (partitionKey verify, etag concurrency, no truncation).
    [TestClass]
    public class FileSystem_Security_Tests
    {
        private static async Task<(IBlobContainer<SampleBlob>, string)> NewContainerAsync()
        {
            string basePath = Path.Combine(Path.GetTempPath(), "pp_fs_" + Guid.NewGuid().ToString("N"));
            IBlobStore store = new FileSystem_BlobStore(basePath);
            var container = await store.CreateContainer<SampleBlob>("c");
            return (container, basePath);
        }

        [TestMethod]
        public async Task Find_WithTraversalId_Throws()
        {
            var (container, basePath) = await NewContainerAsync();
            try
            {
                await Assert.ThrowsExceptionAsync<ArgumentException>(
                    async () => await container.Find("pk", "../../escape.txt"));
            }
            finally { if (Directory.Exists(basePath)) Directory.Delete(basePath, true); }
        }

        [TestMethod]
        public async Task Find_And_Delete_RespectPartitionKey()
        {
            var (container, basePath) = await NewContainerAsync();
            try
            {
                var blob = new SampleBlob { PartitionKey = "p1", id = "a", contentType = "text/plain", fileName = "a.txt" };
                using (var content = new MemoryStream(Encoding.UTF8.GetBytes("v1")))
                    await container.Upload(blob, content);

                Assert.IsNotNull(await container.Find("p1", "a")); // right partition
                Assert.IsNull(await container.Find("p2", "a"));    // wrong partition -> not found
                await Assert.ThrowsExceptionAsync<NotFoundException>(async () => await container.Delete("p2", "a"));
                Assert.IsNotNull(await container.Find("p1", "a")); // wrong-partition delete left it
            }
            finally { if (Directory.Exists(basePath)) Directory.Delete(basePath, true); }
        }

        [TestMethod]
        public async Task UpdateContent_WithStaleEtag_Throws_AndKeepsOldContent()
        {
            var (container, basePath) = await NewContainerAsync();
            try
            {
                var blob = new SampleBlob { PartitionKey = "p1", id = "a", contentType = "text/plain", fileName = "a.txt" };
                using (var content = new MemoryStream(Encoding.UTF8.GetBytes("v1")))
                    await container.Upload(blob, content);

                var stale = new SampleBlob { PartitionKey = "p1", id = "a", etag = "stale-etag" };
                using (var content = new MemoryStream(Encoding.UTF8.GetBytes("v2")))
                    await Assert.ThrowsExceptionAsync<ConcurrencyConflictException>(async () => await container.UpdateContent(stale, content));

                // the old content is intact (no truncation, no overwrite on a rejected update)
                using var stream = await container.Download(await container.Find("p1", "a"));
                using var reader = new StreamReader(stream);
                Assert.AreEqual("v1", reader.ReadToEnd());
            }
            finally { if (Directory.Exists(basePath)) Directory.Delete(basePath, true); }
        }
    }
}
