 
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
	/// ICollection interface for generic entities.
	/// This interface defines basic CRUD operations for working with collections of entities in the data store.
	/// The operations include inserting, updating, upserting, and deleting entities, as well as finding one entity by its ID.
	/// Each method is asynchronous and returns an IResult indicating the outcome of the operation.
	public interface ICollection<TEntity>
		where TEntity: IEntity
	{
		/// Asynchronous method to insert a single entity into the collection.
		/// The 'entity' parameter is of type TEntity, which represents the entity to be inserted into the collection.
		///
		/// If the entity has a pre-set ID, that ID will be used for the insert operation.
		/// If the ID is not set, it will be automatically generated and assigned to the entity.
		/// This ensures that each entity is assigned a unique identifier when inserted.
		public Task<IResult> InsertOne( TEntity entity );
		/// Asynchronous method to update a single entity in the collection.
		/// The 'entity' parameter is of type TEntity, which represents the entity to be updated.
		///
		/// The entity must have its ID already set for the update operation.
		/// The existing entity in the collection with the same ID will be updated with the new values.
		/// If no entity with the specified ID exists, the operation will likely fail.
		public Task<IResult> UpdateOne( TEntity entity );
		/// Asynchronous method to delete a single entity from the collection, identified by its key.
		/// The 'entity' parameter is of type TEntity, and it represents the entity to be deleted.
		///
		/// The entity must have a valid ID to perform the delete operation.
		/// If no entity with the specified ID exists, the operation will likely fail.
		public Task<IResult> DeleteOne( TEntity entity );
		/// Asynchronous method to delete a single entity from the collection, identified by its ID.
		/// The 'id' parameter is of type 'string', representing the ID of the entity to be deleted.
		/// The 'PartitionKey' property represents the key used to partition data in distributed data stores.
		///
		/// The delete operation will search for an entity by the specified ID and remove it from the collection.
		/// If no entity with the specified ID exists, the operation will likely fail.
		public Task<IResult> DeleteOne( string id, string partitionKey );
		/// Asynchronous method to find a single entity in the collection by its ID.
		/// The 'id' parameter is of type 'string', representing the ID of the entity to be found.
		/// The 'PartitionKey' property represents the key used to partition data in distributed data stores.
		///
		/// The method will return the entity matching the specified ID if it exists, otherwise it will return null or an error result.
		public Task<TEntity> FindOne( string id, string partitionKey );
	}
}
