using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.DocumentStore.MongoDB;


namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_Database : IDataStore
    {
        internal readonly IMongoDatabase _mongoDatabase;
        internal readonly GridFS_Connection _connection;

        public GridFS_Database(IMongoDatabase mongoDatabase, GridFS_Connection connection)
        {
            _mongoDatabase = mongoDatabase;
            _connection = connection;
        }

        /// <inheritdoc/>
        IConnection IDataStore.Connection => _connection;
        /// <inheritdoc/>
        IDataStore.StorageModels IDataStore.StorageModel => IDataStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IDataStore.ProviderName => "GridFS";
        /// <inheritdoc/>
        string IDataStore.Name => _mongoDatabase.DatabaseNamespace.DatabaseName;

        /// <inheritdoc/>
        async Task<bool> IDataStore.IsCollectionExists(string collectionName)
        {
            IAsyncCursor<string> collectionCursor = await _mongoDatabase.ListCollectionNamesAsync();
            collectionCursor.MoveNext();

            IEnumerable<string> collections = collectionCursor.Current;

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = collectionName });

            return collections.Contains(gridFSBucket.Options.BucketName + ".chunks")
                && collections.Contains(gridFSBucket.Options.BucketName + ".files")
                && collections.Contains(gridFSBucket.Options.BucketName + ".metadata");
        }

        /// <inheritdoc/>
        async Task<ICollection<TEntity>> IDataStore.GetCollectionByName<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                return null;

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = collectionName });
            return new GridFS_Collection<TEntity>(gridFSBucket, this);
        }

        /// <inheritdoc/>
        async Task<ICollection<TEntity>> IDataStore.CreateCollection<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == true)
                throw new Exception($"Collection '{collectionName}' is already exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            await _mongoDatabase.CreateCollectionAsync(collectionName).ConfigureAwait(false);

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = collectionName });
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".files").ConfigureAwait(false);
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".chunks").ConfigureAwait(false);
            await _mongoDatabase.CreateCollectionAsync(gridFSBucket.Options.BucketName + ".metadata").ConfigureAwait(false);
            return new GridFS_Collection<TEntity>(gridFSBucket, this);
        }

        /// <inheritdoc/>
        async Task IDataStore.DropCollection(string collectionName)
        {
            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            GridFSBucket gridFSBucket = new GridFSBucket(_mongoDatabase, new GridFSBucketOptions() { BucketName = collectionName });
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".files").ConfigureAwait(false);
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".chunks").ConfigureAwait(false);
            await _mongoDatabase.DropCollectionAsync(gridFSBucket.Options.BucketName + ".metadata").ConfigureAwait(false);
        }
    }
}
