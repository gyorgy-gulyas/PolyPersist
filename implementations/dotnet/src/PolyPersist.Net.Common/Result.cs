namespace PolyPersist.Net.Common
{
    /// <inheritdoc />
    public class Result : IResult
    {
        /// <inheritdoc />
        public bool Succeeded { get; set; }

        /// <inheritdoc />
        public string Message { get; set; }

        /// <inheritdoc />
        public IDictionary<string, string> OperationValues { get; set; }

        public static Result Ok(IDictionary<string, string> operationValues = null) => new() { 
            Succeeded = true, 
            Message = "OK", 
            OperationValues = operationValues ?? new Dictionary<string,string>() 
        };

        public static Result Error( string message, IDictionary<string, string> operationValues = null ) => new() { 
            Succeeded = false, 
            Message = message, 
            OperationValues = operationValues ?? new Dictionary<string, string>() 
        };
    }

}
