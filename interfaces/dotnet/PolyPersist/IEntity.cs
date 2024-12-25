 
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
	/// IEntity interface represents a generic entity with an ID and etag.
	/// The 'etag' is used for versioning or concurrency control in databases, often representing the entity's current version.
	/// The 'id' is the unique identifier for the entity within the collection or data store.
	/// These properties ensure that each entity has a unique identifier and version for data integrity.
	public interface IEntity
	{
		/// The 'id' property represents the unique identifier for the entity.
		/// This ID is typically used to reference and retrieve the entity from the data store.
		public string id { get; set;}
		/// The 'etag' property represents the entity's version tag or a unique identifier used for concurrency control.
		/// It is typically used to detect changes or updates in the entity, ensuring that the correct version of the entity is being modified.
		public string etag { get; set;}
		/// The 'PartitionKey' property represents the key used to partition data in distributed data stores.
		/// This allows for more efficient storage and retrieval of entities, especially in large-scale, partitioned systems.
		public string PartitionKey { get; }
	}
}
