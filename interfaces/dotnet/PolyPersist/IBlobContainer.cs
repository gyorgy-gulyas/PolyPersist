 
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
	/// The IBlobContainer interface defines operations for managing blobs within a specific container.
	/// It provides functionality for uploading, downloading, deleting, and updating blobs, as well as retrieving metadata.
	///
	/// Examples:
	/// - A container for user-uploaded photos, where each blob represents an image.
	/// - A container for logs, where each blob is a log file.
	public interface IBlobContainer<TBlob>
		where TBlob: IBlob
	{
		/// Read-only property representing the name of the container.
		///
		/// Example:
		/// If the container is named 'UserFiles', this property holds the value 'UserFiles'.
		public string Name { get; }

		/// Uploads a blob and its content to the container.
		///
		/// Parameters:
		/// - blob: The blob entity to upload.
		/// - content: The stream containing the blob's content.
		///
		/// Use Case:
		/// Add a new file to the container with its associated metadata.
		public Task Upload( TBlob blob, Stream content );
		/// Downloads the content of a blob from the container.
		///
		/// Parameters:
		/// - blob: The blob entity to download.
		///
		/// Returns:
		/// - A stream containing the blob's content.
		///
		/// Use Case:
		/// Retrieve a file for viewing or processing.
		public Task<Stream> Download( TBlob blob );
		/// Deletes a blob from the container.
		///
		/// Parameters:
		/// - partitionKey: The partition key of the blob.
		/// - id: The unique identifier of the blob.
		///
		/// Use Case:
		/// Remove a file that is no longer needed from the container.
		public Task Delete( string partitionKey, string id );
		/// Finds a blob in the container by its partition key and ID.
		///
		/// Parameters:
		/// - partitionKey: The partition key of the blob.
		/// - id: The unique identifier of the blob.
		///
		/// Returns:
		/// - A stream containing the blob's content.
		///
		/// Use Case:
		/// Locate and retrieve a specific file from the container.
		public Task<TBlob> Find( string partitionKey, string id );
		/// Updates the content of an existing blob in the container.
		///
		/// Parameters:
		/// - blob: The blob entity to update.
		/// - content: The stream containing the new content.
		///
		/// Use Case:
		/// Modify the content of an existing file, such as updating a document.
		public Task UpdateContent( TBlob blob, Stream content );
		/// Updates the metadata of an existing blob in the container.
		///
		/// Parameters:
		/// - blob: The blob entity whose metadata is to be updated.
		///
		/// Use Case:
		/// Modify metadata, such as changing the description or tags associated with a file.
		public Task UpdateMetadata( TBlob blob );
		/// Retrieves the underlying implementation of the blob container.
		///
		/// Returns:
		/// - A value of type `any` that provides direct access to the underlying storage implementation.
		///
		/// Use Case:
		/// Use this method for advanced operations that are not covered by the standard interface.
		/// Note: This method should be used cautiously, as the returned object is implementation-specific.
		public object GetUnderlyingImplementation(  );
	}
}
