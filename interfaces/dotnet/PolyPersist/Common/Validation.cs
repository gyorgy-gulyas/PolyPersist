using PolyPersist;

namespace PolyPersist.Net.Common
{
    public class Validator
    {
        static public void Validate(IValidable validabale)
        {
            IList<IValidationError> validationErrors = [];
            if (validabale.Validate(validationErrors) == false)
                throw new ValidationExeption(validationErrors);
        }
    }

    public class ValidationError : IValidationError
    {
        public string TypeOfEntity { get; set; } = null!;
        public string MemberOfEntity { get; set; } = null!;

        // Where the failure is, relative to the object Validate was called on: "quantity" for the
        // object's own member, "items[1].quantity" once the walk goes into a value object.
        public string Path { get; set; } = string.Empty;

        public string ErrorText { get; set; } = null!;
    }

    public class ValidationExeption : PolyPersistException
    {
        public IList<IValidationError> ValidationErrors { get; }

        public ValidationExeption(IList<IValidationError> validationErrors)
            : base(string.Join("\n", validationErrors.Select(e => $"{e.TypeOfEntity} => {e.ErrorText}")))
        {
            ValidationErrors = validationErrors;
        }
    }
}
