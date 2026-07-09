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
        public string ErrorText { get; set; } = null!;
    }

    public class ValidationExeption : Exception
    {
        public IList<IValidationError> ValidationErrors { get; }

        public ValidationExeption(IList<IValidationError> validationErrors)
            : base(string.Join("\n", validationErrors.Select(e => $"{e.TypeOfEntity} => {e.ErrorText}")))
        {
            ValidationErrors = validationErrors;
        }
    }
}
