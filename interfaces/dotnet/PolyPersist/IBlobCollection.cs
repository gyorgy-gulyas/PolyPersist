 
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
	public interface IBlobCollection<TBlob> : ICollection<TBlob>
		where TBlob: IEntity, new()
	{
		/// The 'UploadContent' method is responsible for reading the file's binary or textual content
		/// from the given 'source' stream and storing it in the data store.
		///
		/// This method should handle the reading of the entire 'source' stream
		/// and associate its contents with the underlying data record for this file.
		/// Implementations may perform additional tasks such as validating file size or type before saving.
		public Task UploadContent( TBlob entity, Stream source );
		/// The 'DownloadContentTo' method is responsible for retrieving the file's content
		/// from the data store and writing it to the provided 'destination' stream.
		///
		/// This method should read the entire stored file content and write it to 'destination'.
		/// It is typically used whenever an application needs to serve or transfer the file to another medium,
		/// such as returning it in an HTTP response for download, or saving it to local disk.
		public Task DownloadContentTo( TBlob entity, Stream destination );
	}
}
