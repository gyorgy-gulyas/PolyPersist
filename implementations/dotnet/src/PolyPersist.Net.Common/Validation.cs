namespace PolyPersist.Net.Common
{
    public class Validator
    {
        async static public Task<Result> Validate(IValidabale validabale)
        {
            IList<IValidationError> validationErrors = [];
            if (await validabale.Validate(validationErrors) == false)
                return new ValidationResult(validationErrors);

            return Result.Ok();
        }
    }

    public class ValidationError : IValidationError
    {
        public string TypeOfEntity { get; set; }
        public string MemberOfEntity { get; set; }
        public string ErrorText { get; set; }
    }

    public class ValidationResult : Result
    {
        public IList<IValidationError> ValidationErrors { get; }

        public ValidationResult(IList<IValidationError> validationErrors)
        {
            ValidationErrors = validationErrors;
        }
    }
}
