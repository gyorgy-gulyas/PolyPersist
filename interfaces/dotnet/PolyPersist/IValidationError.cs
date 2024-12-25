 
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
	/// IValidationError is an interface that represents a validation error occurring during the validation process.
	/// It provides structured details about the nature and location of the error within the entity.
	public interface IValidationError
	{
		/// Represents the type or class name of the entity where the validation error occurred.
		/// For example, it might be "User", "Order", or "Product".
		public string TypeOfEntity { get; }
		/// Indicates the specific member or property of the entity that caused the validation error.
		/// For example, it could be "Username", "OrderDate", or "Price".
		public string MemberOfEntity { get; }
		/// A human-readable description of the error, explaining what went wrong.
		/// This is useful for displaying error messages to users or logging them for debugging.
		public string ErrorText { get; }
	}
}
