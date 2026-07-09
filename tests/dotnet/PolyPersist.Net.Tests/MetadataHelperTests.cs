using System.Globalization;
using PolyPersist.Net.Common;
using PolyPersist.Net.Attributes;
using System.Text.Json.Serialization;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class MetadataHelperTests
    {
        public class MetaPoco
        {
            public string Name { get; set; } = "";
            public int Count { get; set; }
            public bool Flag { get; set; }
            public double Ratio { get; set; }
            public long Big { get; set; }
            public decimal Amount { get; set; }
            public DateTime When { get; set; }
            public DateOnly Day { get; set; }
            public TimeOnly Time { get; set; }
            public Color Shade { get; set; }
            public string? MaybeNull { get; set; }
            public string ReadOnly => "readonly-value";
            public int PublicField;
        }

        public class IgnorePoco
        {
            public string Kept { get; set; } = "kept";

            [PolyPersist.Net.Attributes.Ignore]
            public string IgnoredByOwn { get; set; } = "own";

            [JsonIgnore]
            public string IgnoredByJson { get; set; } = "json";
        }

        private static MetaPoco MakeSample() => new()
        {
            Name = "hello",
            Count = 42,
            Flag = true,
            Ratio = 4, // whole number: double ToString() is culture-sensitive, keep round-trip stable
            Big = 123456789L,
            Amount = 19.95m,
            When = new DateTime(2026, 7, 9, 10, 30, 0, DateTimeKind.Utc),
            Day = new DateOnly(2026, 7, 9),
            Time = new TimeOnly(13, 45, 0),
            Shade = Color.Green,
            MaybeNull = null,
            PublicField = 7,
        };

        [TestMethod]
        public void GetMetadata_NullEntity_Throws()
        {
            MetaPoco? entity = null;
            Assert.ThrowsException<ArgumentNullException>(() => MetadataHelper.GetMetadata(entity!));
        }

        [TestMethod]
        public void GetMetadata_FormatsAllTypes()
        {
            var meta = MetadataHelper.GetMetadata(MakeSample());

            Assert.AreEqual("hello", meta["Name"]);
            Assert.AreEqual("42", meta["Count"]);
            Assert.AreEqual("True", meta["Flag"]);
            Assert.AreEqual("19.95", meta["Amount"]);
            Assert.AreEqual("Green", meta["Shade"]);
            Assert.AreEqual(string.Empty, meta["MaybeNull"]); // null -> empty
            Assert.AreEqual("readonly-value", meta["ReadOnly"]);
            Assert.AreEqual("7", meta["PublicField"]);
            // DateTime uses round-trip "o" format
            StringAssert.Contains(meta["When"], "2026-07-09");
        }

        [TestMethod]
        public void SetMetadata_FromStringDictionary_RoundTrips()
        {
            var original = MakeSample();
            var meta = MetadataHelper.GetMetadata(original);

            var restored = MetadataHelper.SetMetadata(new MetaPoco(), meta);

            Assert.AreEqual(original.Name, restored.Name);
            Assert.AreEqual(original.Count, restored.Count);
            Assert.AreEqual(original.Flag, restored.Flag);
            Assert.AreEqual(original.Ratio, restored.Ratio);
            Assert.AreEqual(original.Big, restored.Big);
            Assert.AreEqual(original.Amount, restored.Amount);
            Assert.AreEqual(original.When, restored.When);
            Assert.AreEqual(original.Day, restored.Day);
            Assert.AreEqual(original.Time, restored.Time);
            Assert.AreEqual(original.Shade, restored.Shade);
            Assert.AreEqual(original.PublicField, restored.PublicField);
        }

        [TestMethod]
        public void SetMetadata_NullStringDictionary_Throws()
        {
            IDictionary<string, string>? meta = null;
            Assert.ThrowsException<ArgumentNullException>(
                () => MetadataHelper.SetMetadata(new MetaPoco(), meta!));
        }

        [TestMethod]
        public void SetMetadata_FromObjectDictionary_SetsValues()
        {
            var meta = new Dictionary<string, object>
            {
                ["Name"] = "world",
                ["Count"] = 5,
                ["Flag"] = true,
                ["PublicField"] = 9,
            };

            var poco = MetadataHelper.SetMetadata(new MetaPoco(), meta);

            Assert.AreEqual("world", poco.Name);
            Assert.AreEqual(5, poco.Count);
            Assert.IsTrue(poco.Flag);
            Assert.AreEqual(9, poco.PublicField);
        }

        [TestMethod]
        public void SetMetadata_NullObjectDictionary_Throws()
        {
            IDictionary<string, object>? meta = null;
            Assert.ThrowsException<ArgumentNullException>(
                () => MetadataHelper.SetMetadata(new MetaPoco(), meta!));
        }

        [TestMethod]
        public void SetMetadata_SingleField_SetsWhenPresent_NoOpWhenMissing()
        {
            var poco = MetadataHelper.SetMetadata(new MetaPoco(), "Name", "single");
            Assert.AreEqual("single", poco.Name);

            // Unknown field name -> no-op, no throw
            var poco2 = MetadataHelper.SetMetadata(new MetaPoco(), "DoesNotExist", "x");
            Assert.AreEqual("", poco2.Name);
        }

        [TestMethod]
        public void SetMetadata_SingleField_Typed()
        {
            var built = MetadataHelper.SetMetadata(new MetaPoco(), "Count", 11);
            Assert.AreEqual(11, built.Count);
        }

        [TestMethod]
        public void GetAccessors_NormalAndLowerCase()
        {
            var normal = MetadataHelper.GetAccessors<MetaPoco>();
            Assert.IsTrue(normal.ContainsKey("Name"));
            Assert.IsTrue(normal.ContainsKey("PublicField"));

            var lower = MetadataHelper.GetAccessors<MetaPoco>(lowerCaseNames: true);
            Assert.IsTrue(lower.ContainsKey("name"));
            Assert.IsTrue(lower.ContainsKey("publicfield"));

            // read-only property has no setter
            Assert.IsNull(normal["ReadOnly"].Setter);
            Assert.IsNotNull(normal["Name"].Setter);
            Assert.AreEqual(typeof(string), normal["Name"].Type);
        }

        [TestMethod]
        public void GetMetadata_HonorsIgnoreAttributes()
        {
            var meta = MetadataHelper.GetMetadata(new IgnorePoco());
            Assert.IsTrue(meta.ContainsKey("Kept"));
            Assert.IsFalse(meta.ContainsKey("IgnoredByOwn"));
            Assert.IsFalse(meta.ContainsKey("IgnoredByJson"));
        }

        [TestMethod]
        public void MemberAccessor_GetterAndSetterWork()
        {
            var accessors = MetadataHelper.GetAccessors<MetaPoco>();
            var poco = new MetaPoco();
            accessors["Name"].Setter!(poco, "abc");
            Assert.AreEqual("abc", accessors["Name"].Getter(poco));
        }
    }
}
