using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.DocumentStore.MongoDB;


namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_BlobStore : IBlobStore
    {
        internal readonly IMongoDatabase _mongoDatabase;

        public GridFS_BlobStore(IMongoDatabase mongoDatabase )
        {
            _mongoDatabase = mongoDatabase;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "GridFS";
        /// <inheritdoc/>
        string IStore.Name => _mongoDatabase.DatabaseNamespace.DatabaseName;

        /// <inheritdoc/>
        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            IAsyncCursor<string> collectionCursor = await _mongoDatabase.ListCollectionNamesAsync();
            collectionCursor.MoveNext();

            IEnumerable<string> collections = collectionCursor.Current;

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = containerName });

            return collections.Contains(gridFSBucket.Options.BucketName + ".chunks")
                && collections.Contains(gridFSBucket.Options.BucketName + ".files")
                && collections.Contains(gridFSBucket.Options.BucketName + ".metadata");
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TEntity>> IBlobStore.GetContainerByName<TEntity>(string containerName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IBlobStore).IsContainerExists(containerName).ConfigureAwait(false) == false)
                return null;

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = containerName });
            return new GridFS_BlobContainer<TEntity>(gridFSBucket, this);
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TEntity>> IBlobStore.CreateContainer<TEntity>(string containerName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IBlobStore).IsContainerExists(containerName).ConfigureAwait(false) == true)
                throw new Exception($"Container '{containerName}' is already exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = containerName });
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".files").ConfigureAwait(false);
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".chunks").ConfigureAwait(false);
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".metadata").ConfigureAwait(false);
            return new GridFS_BlobContainer<TEntity>(gridFSBucket, this);
        }

        /// <inheritdoc/>
        async Task IBlobStore.DropContainer(string containerName)
        {
            if (await (this as IBlobStore).IsContainerExists(containerName).ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = containerName });
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".files").ConfigureAwait(false);
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".chunks").ConfigureAwait(false);
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".metadata").ConfigureAwait(false);
        }
    }
}
