using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;

namespace PolyPersist.Net.BlobStore.GoogleCloudStorage
{
    public class GoogleCloudStorage_BlobStore : IBlobStore
    {
        internal readonly StorageService _gcsService;
        internal readonly GoogleCloudStorageConnectionInfo _config;

        public GoogleCloudStorage_BlobStore(string connectionString)
        {
            _config = GoogleCloudStorageConnectionStringParser.Parse(connectionString);

            GoogleCredential credential = _config.UseFakeToken
                ? GoogleCredential.FromAccessToken("fake-token")
                : (!string.IsNullOrWhiteSpace(_config.CredentialPath)
                    ? GoogleCredential.FromFile(_config.CredentialPath)
                    : GoogleCredential.GetApplicationDefault());

            _gcsService = new StorageService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
                BaseUri = string.IsNullOrWhiteSpace(_config.BaseUrl) ? null : $"{_config.BaseUrl}/storage/v1/",
                ApplicationName = "GCSClient"
            });
        }

        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        string IStore.ProviderName => "GCS_Blobs";

        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            try
            {
                var request = _gcsService.Buckets.Get(containerName);
                var bucket = await request.ExecuteAsync().ConfigureAwait(false);
                return bucket != null;
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch
            {
                throw;
            }
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            if (await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' already exists in Google Cloud Storage");

            try
            {
                var insertRequest = _gcsService.Buckets.Insert(new Bucket { Name = containerName }, _config.ProjectId);
                await insertRequest.ExecuteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to create bucket '{containerName}': {ex.Message}", ex);
            }

            return new GoogleCloudStorage_BlobContainer<TBlob>(containerName, _gcsService, _config.ProjectId);
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in Google Cloud Storage");

            return new GoogleCloudStorage_BlobContainer<TBlob>(containerName, _gcsService, _config.ProjectId);
        }

        async Task IBlobStore.DropContainer(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in Google Cloud Storage");

            try
            {
                var listRequest = _gcsService.Objects.List(containerName);
                var listResponse = await listRequest.ExecuteAsync().ConfigureAwait(false);

                if (listResponse.Items != null)
                {
                    foreach (var obj in listResponse.Items)
                    {
                        var deleteRequest = _gcsService.Objects.Delete(containerName, obj.Name);
                        await deleteRequest.ExecuteAsync().ConfigureAwait(false);
                    }
                }

                var deleteBucketRequest = _gcsService.Buckets.Delete(containerName);
                await deleteBucketRequest.ExecuteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to delete container '{containerName}': {ex.Message}", ex);
            }
        }
    }

    public class GoogleCloudStorageConnectionInfo
    {
        public string ProjectId { get; set; }
        public string CredentialPath { get; set; }
        public string BaseUrl { get; set; }
        public bool UseFakeToken { get; set; }
    }

    public static class GoogleCloudStorageConnectionStringParser
    {
        public static GoogleCloudStorageConnectionInfo Parse(string connectionString)
        {
            var dict = connectionString
                .Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Split('='))
                .Where(x => x.Length == 2)
                .ToDictionary(x => x[0].Trim().ToLowerInvariant(), x => x[1].Trim(), StringComparer.OrdinalIgnoreCase);

            return new GoogleCloudStorageConnectionInfo
            {
                ProjectId = dict.TryGetValue("projectid", out var pid) ? pid : null,
                CredentialPath = dict.TryGetValue("credentialpath", out var cred) ? cred : null,
                BaseUrl = dict.TryGetValue("baseurl", out var url) ? url : null,
                UseFakeToken = dict.TryGetValue("usetoken", out var tok) && tok.Equals("fake", StringComparison.OrdinalIgnoreCase)
            };
        }
    }
}
