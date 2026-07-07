using Microsoft.VisualStudio.TestTools.UnitTesting;
using PolyPersist;
using PolyPersist.Net.BlobStore.FileSystem;
using System;
using System.IO;
using System.Threading.Tasks;

namespace PolyPersist.Net.BlobStore.Tests
{
    // PP-02: the FileSystem blob store must not let a path-traversal id escape the container.
    [TestClass]
    public class FileSystem_Security_Tests
    {
        [TestMethod]
        public async Task Find_WithTraversalId_Throws()
        {
            string basePath = Path.Combine(Path.GetTempPath(), "pp_fs_sec_" + Guid.NewGuid().ToString("N"));
            IBlobStore store = new FileSystem_BlobStore(basePath);
            IBlobContainer<SampleBlob> container = await store.CreateContainer<SampleBlob>("sec");
            try
            {
                await Assert.ThrowsExceptionAsync<ArgumentException>(
                    async () => await container.Find("pk", "../../escape.txt"));
            }
            finally
            {
                if (Directory.Exists(basePath))
                    Directory.Delete(basePath, true);
            }
        }
    }
}
