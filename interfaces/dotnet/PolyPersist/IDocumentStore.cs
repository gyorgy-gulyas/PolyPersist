 
// <auto-generated>
//     This code was generated by unicontract
//     see more information: https://github.com/gyorgy-gulyas/UniContract
//
//     Changes to this file may cause incorrect behavior and will be lost if the code is regenerated.
// </auto-generated>

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace PolyPersist
{
	public interface IDocumentStore : IStore
	{
		/// Asynchronous method to check if a collection exists in the data store.
		/// It returns a boolean value, indicating whether the collection with the specified name exists or not.
		/// This can be used to verify the presence of a collection before performing operations like retrieval or deletion.
		///
		/// @param collectionName - The name of the collection to check for existence.
		/// @returns A boolean value indicating if the collection is present in the data store.
		public Task<bool> IsCollectionExists( string collectionName );
		/// Asynchronous method to retrieve a collection by its name from the data store.
		/// This method returns the collection as an ICollection of entities or files or vectors.
		/// 
		/// @param collectionName - The name of the collection to retrieve from the data store.
		/// @returns An ICollection representing the collection with the specified name.
		public Task<IDocumentCollection<TDocument>> GetCollectionByName<TDocument>( string collectionName ) where TDocument: IDocument, new();
		/// Asynchronous method to create a new collection in the data store.
		/// This method creates a collection with the specified name and returns it as an ICollection of entities of files,
		///
		/// @param collectionName - The name of the new collection to be created.
		/// @returns The newly created collection, represented as an ICollection
		public Task<IDocumentCollection<TDocument>> CreateCollection<TDocument>( string collectionName ) where TDocument: IDocument, new();
		/// Asynchronous method to drop (delete) an existing collection from the data store.
		/// This method removes the specified collection, returning a boolean indicating whether the operation was successful.
		/// The collection is represented as an ICollection of entities, files or vectors 
		///
		/// @param collectionName - The name of the collection to be dropped from the data store.
		public Task DropCollection( string collectionName );
	}
}
