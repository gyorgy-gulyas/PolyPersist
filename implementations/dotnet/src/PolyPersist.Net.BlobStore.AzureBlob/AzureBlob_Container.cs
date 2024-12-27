using Azure.Storage.Blobs;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_Container<TFile> : IFileCollection<TFile>
        where TFile : IFile
    {
        private BlobContainerClient _containerClient;

        public AzureBlob_Container(BlobContainerClient containerClient)
        {
            _containerClient = containerClient;
        }

        async Task IFileCollection<TFile>.Delete(TFile file)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(file.fileName, file.PartitionKey));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"File'{typeof(File).Name}' {file.fileName} can not be deleted because it is already removed");

            await blobClient.DeleteAsync().ConfigureAwait(false);
        }

        async Task IFileCollection<TFile>.Upload(string fileName, string partitionKey, Stream content)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(fileName, partitionKey));
            await blobClient.UploadAsync(content, overwrite: true).ConfigureAwait(false);
        }

        async Task<Stream> IFileCollection<TFile>.Download(string fileName, string partitionKey)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(fileName, partitionKey));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"File'{typeof(File).Name}' {fileName} des not exist");

            using MemoryStream memoryStream = new();
            await blobClient.DownloadToAsync(memoryStream);

            return memoryStream;
        }

        async Task<bool> IFileCollection<TFile>.IsExists(string fileName, string partitionKey)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(fileName, partitionKey));
            return await blobClient.ExistsAsync().ConfigureAwait(false);
        }

        private string _makePath(string fileName, string partitionKey)
            => $"{partitionKey}/{fileName}";
    }
}
