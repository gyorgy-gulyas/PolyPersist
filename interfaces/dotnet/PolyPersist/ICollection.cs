 
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
	/// The ICollection interface defines methods for CRUD operations on entities.
	/// This interface is used for collections that store entities.
	public interface ICollection<TEntity>
		where TEntity: IEntity, new()
	{
		/// Read-only property representing the name of the collection.
		public string Name { get; }

		/// Asynchronous method to insert an entity into the collection.
		/// The 'entity' parameter is the entity that will be inserted into the collection.
		public Task Insert( TEntity entity );
		/// Asynchronous method to update an existing entity in the collection.
		/// The 'entity' parameter represents the entity to be updated in the collection.
		public Task Update( TEntity entity );
		/// Asynchronous method to delete an entity from the collection.
		/// The 'entity' parameter represents the entity that will be deleted.
		public Task Delete( TEntity entity );
		/// Asynchronous method to delete an entity using its ID and PartitionKey.
		/// The 'id' parameter is the unique identifier of the entity, and 'partitionKey' is used for partitioning the data in distributed data stores.
		public Task Delete( string id, string partitionKey );
		/// Asynchronous method to find an entity by its ID and PartitionKey.
		/// The 'id' parameter is the unique identifier of the entity, and 'partitionKey' is used to partition data.
		/// Returns the entity if found, or null if not found.
		public Task<TEntity> Find( string id, string partitionKey );
		/// Asynchronous method to query the entiies
		/// the return value generic, so the implementation can define what are the real types
		/// In dotnet is can be IQueryable, in java it can be Java Streams, or Querydsl etc...
		public TQuery Query<TQuery>(  );
		/// getting the underlying implementation
		/// please use this method carefully, because the returned value is different in every implementation
		public object GetUnderlyingImplementation(  );
	}
}
