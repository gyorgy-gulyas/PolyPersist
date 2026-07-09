using PolyPersist.Net.Common;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class ValidationTests
    {
        [TestMethod]
        public void Validate_Valid_DoesNotThrow()
        {
            var e = new ValidableEntity { IsValid = true };
            Validator.Validate(e); // no throw
        }

        [TestMethod]
        public void Validate_Invalid_ThrowsWithErrors()
        {
            var e = new ValidableEntity { IsValid = false, ErrorText = "bad name" };
            var ex = Assert.ThrowsException<ValidationExeption>(() => Validator.Validate(e));

            Assert.AreEqual(1, ex.ValidationErrors.Count);
            Assert.AreEqual("ValidableEntity", ex.ValidationErrors[0].TypeOfEntity);
            Assert.AreEqual("bad name", ex.ValidationErrors[0].ErrorText);
            StringAssert.Contains(ex.Message, "bad name");
        }

        [TestMethod]
        public void ValidationError_Properties()
        {
            var err = new ValidationError
            {
                TypeOfEntity = "T",
                MemberOfEntity = "M",
                ErrorText = "E",
            };
            Assert.AreEqual("T", err.TypeOfEntity);
            Assert.AreEqual("M", err.MemberOfEntity);
            Assert.AreEqual("E", err.ErrorText);
        }
    }

    [TestClass]
    public class CollectionCommonTests
    {
        private static ValidableEntity ValidInsertEntity() =>
            new() { id = "1", PartitionKey = "pk", etag = "", IsValid = true };

        [TestMethod]
        public void CheckBeforeInsert_Happy()
        {
            CollectionCommon.CheckBeforeInsert(ValidInsertEntity());
        }

        [TestMethod]
        public void CheckBeforeInsert_PlainEntity_Happy()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "pk", etag = "" };
            CollectionCommon.CheckBeforeInsert(e);
        }

        [TestMethod]
        public void CheckBeforeInsert_InvalidEntity_ThrowsValidation()
        {
            var e = new ValidableEntity { id = "1", PartitionKey = "pk", etag = "", IsValid = false };
            Assert.ThrowsException<ValidationExeption>(() => CollectionCommon.CheckBeforeInsert(e));
        }

        [TestMethod]
        public void CheckBeforeInsert_MissingPartitionKey_Throws()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "", etag = "" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckBeforeInsert(e));
            StringAssert.Contains(ex.Message, "PartitionKey");
        }

        [TestMethod]
        public void CheckBeforeInsert_EtagAlreadySet_Throws()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "pk", etag = "already" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckBeforeInsert(e));
            StringAssert.Contains(ex.Message, "ETag");
        }

        [TestMethod]
        public void CheckBeforeUpdate_Happy()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "pk", etag = "e1" };
            CollectionCommon.CheckBeforeUpdate(e);
        }

        [TestMethod]
        public void CheckBeforeUpdate_InvalidEntity_ThrowsValidation()
        {
            var e = new ValidableEntity { id = "1", PartitionKey = "pk", etag = "e1", IsValid = false };
            Assert.ThrowsException<ValidationExeption>(() => CollectionCommon.CheckBeforeUpdate(e));
        }

        [TestMethod]
        public void CheckBeforeUpdate_MissingPartitionKey_Throws()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "", etag = "e1" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckBeforeUpdate(e));
            StringAssert.Contains(ex.Message, "PartitionKey");
        }

        [TestMethod]
        public void CheckBeforeUpdate_MissingEtag_Throws()
        {
            var e = new PlainEntity { id = "1", PartitionKey = "pk", etag = "" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckBeforeUpdate(e));
            StringAssert.Contains(ex.Message, "ETag");
        }

        [TestMethod]
        public void CheckEtagMatch_Entity_Happy()
        {
            var stored = new PlainEntity { id = "1", etag = "e1" };
            var incoming = new PlainEntity { id = "1", etag = "e1" };
            CollectionCommon.CheckEtagMatch(stored, incoming);
        }

        [TestMethod]
        public void CheckEtagMatch_NullStored_Throws()
        {
            PlainEntity? stored = null;
            var incoming = new PlainEntity { id = "1", etag = "e1" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckEtagMatch(stored, incoming));
            StringAssert.Contains(ex.Message, "does not exist");
        }

        [TestMethod]
        public void CheckEtagMatch_Entity_Mismatch_Throws()
        {
            var stored = new PlainEntity { id = "1", etag = "old" };
            var incoming = new PlainEntity { id = "1", etag = "new" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckEtagMatch(stored, incoming));
            StringAssert.Contains(ex.Message, "already changed");
        }

        [TestMethod]
        public void CheckEtagMatch_StringOverload_Happy()
        {
            var incoming = new PlainEntity { id = "1", etag = "e1" };
            CollectionCommon.CheckEtagMatch("e1", incoming);
        }

        [TestMethod]
        public void CheckEtagMatch_StringOverload_Mismatch_Throws()
        {
            var incoming = new PlainEntity { id = "1", etag = "e1" };
            var ex = Assert.ThrowsException<Exception>(() => CollectionCommon.CheckEtagMatch("other", incoming));
            StringAssert.Contains(ex.Message, "already changed");
        }
    }
}
