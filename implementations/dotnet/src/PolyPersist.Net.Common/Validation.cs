using PolyPersist;

namespace PolyPersist.Net.Common
{
    public class Validator
    {
        async static public Task Validate(IValidable validabale)
        {
            IList<IValidationError> validationErrors = [];
            if (await validabale.Validate(validationErrors).ConfigureAwait(false) == false)
                throw new ValidationExeption(validationErrors);
        }
    }

    public class ValidationError : IValidationError
    {
        public string TypeOfEntity { get; set; }
        public string MemberOfEntity { get; set; }
        public string ErrorText { get; set; }
    }

    public class ValidationExeption : Exception
    {
        public IList<IValidationError> ValidationErrors { get; }

        public ValidationExeption(IList<IValidationError> validationErrors)
        {
            ValidationErrors = validationErrors;
        }
    }
}
