 
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
	/// The IBlobStore interface defines operations for managing blob storage containers.
	/// Containers serve as logical units for organizing and isolating blobs.
	///
	/// Examples:
	/// - A container named 'UserUploads' storing all user-uploaded files.
	/// - A container named 'Logs' for storing application logs.
	public interface IBlobStore : IStore
	{
		/// Checks whether a container exists in the blob storage.
		///
		/// Parameters:
		/// - containerName: The name of the container to check.
		///
		/// Returns:
		/// - Boolean value indicating whether the container exists.
		///
		/// Use Case:
		/// Before attempting to access or create a container, check if it already exists.
		public Task<bool> IsContainerExists( string containerName );
		/// Retrieves a container by its name.
		///
		/// Parameters:
		/// - containerName: The name of the container to retrieve.
		///
		/// Returns:
		/// - An instance of IBlobContainer representing the requested container.
		///
		/// Generic Constraint:
		/// - TBlob must implement the IBlob interface, ensuring type safety for blob operations.
		///
		/// Use Case:
		/// Retrieve a container to perform blob operations like uploading or downloading files.
		public Task<IBlobContainer<TBlob>> GetContainerByName<TBlob>( string containerName ) where TBlob: IBlob;
		/// Creates a new container in the blob storage.
		///
		/// Parameters:
		/// - containerName: The name of the container to create.
		///
		/// Returns:
		/// - An instance of IBlobContainer representing the newly created container.
		///
		/// Generic Constraint:
		/// - TBlob must implement the IBlob interface, ensuring type safety for blob operations.
		///
		/// Use Case:
		/// Create a container for a specific group of files, such as user uploads or application logs.
		public Task<IBlobContainer<TBlob>> CreateContainer<TBlob>( string containerName ) where TBlob: IBlob;
		/// Deletes an existing container from the blob storage.
		///
		/// Parameters:
		/// - containerName: The name of the container to delete.
		///
		/// Use Case:
		/// Remove containers that are no longer needed, freeing up storage space and reducing costs.
		public Task DropContainer( string containerName );
	}
}