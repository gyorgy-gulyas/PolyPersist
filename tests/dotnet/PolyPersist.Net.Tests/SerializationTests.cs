using System.Text.Json;
using PolyPersist.Net.Common;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class SerializationTests
    {
        public class SampleBlob : IBlob
        {
            public string id { get; set; } = null!;
            public string etag { get; set; } = null!;
            public string PartitionKey { get; set; } = null!;
            public DateTime LastUpdate { get; set; }
            public string fileName { get; set; } = null!;
            public string contentType { get; set; } = null!;
            public Color Shade { get; set; }
        }

        [TestMethod]
        public void BlobMetadata_RoundTrips()
        {
            var blob = new SampleBlob
            {
                id = "1",
                etag = "e1",
                PartitionKey = "pk",
                LastUpdate = new DateTime(2026, 7, 9, 0, 0, 0, DateTimeKind.Utc),
                fileName = "a.txt",
                contentType = "text/plain",
                Shade = Color.Blue,
            };

            var json = BlobMetadata.Serialize(blob);

            // Stored in metadata headers => must be single line (not indented).
            Assert.IsFalse(json.Contains('\n'));
            // enum-as-string converter present
            StringAssert.Contains(json, "Blue");

            var restored = BlobMetadata.Deserialize<SampleBlob>(json);
            Assert.AreEqual(blob.id, restored.id);
            Assert.AreEqual(blob.fileName, restored.fileName);
            Assert.AreEqual(blob.Shade, restored.Shade);
        }

        [TestMethod]
        public void JsonOptionsProvider_HasEnumConverterAndIndented()
        {
            var options = JsonOptionsProvider.Options();
            Assert.IsTrue(options.WriteIndented);

            var json = JsonSerializer.Serialize(new { Shade = Color.Red }, options);
            StringAssert.Contains(json, "Red");
        }

        [TestMethod]
        public void EntityId_ImplicitConversions()
        {
            EntityId<PlainEntity> id = "abc";       // string -> EntityId
            string back = id;                        // EntityId -> string
            Assert.AreEqual("abc", back);
            Assert.AreEqual("abc", id.Value);
            Assert.AreEqual("abc", id.ToString());
        }

        [TestMethod]
        public void EntityId_ParseAndTryParse()
        {
            var parsed = EntityId<PlainEntity>.Parse("xyz", null);
            Assert.AreEqual("xyz", parsed.Value);

            Assert.IsTrue(EntityId<PlainEntity>.TryParse("q", null, out var ok));
            Assert.AreEqual("q", ok.Value);

            Assert.IsFalse(EntityId<PlainEntity>.TryParse(null, null, out var empty));
            Assert.AreEqual(string.Empty, empty.Value);
        }

        [TestMethod]
        public void EntityId_JsonRoundTripsAsPlainString()
        {
            var id = new EntityId<PlainEntity>("id-123");
            var json = JsonSerializer.Serialize(id);
            Assert.AreEqual("\"id-123\"", json);

            var restored = JsonSerializer.Deserialize<EntityId<PlainEntity>>(json);
            Assert.AreEqual("id-123", restored.Value);
        }

        [TestMethod]
        public void EntityId_JsonRoundTrips_NullBecomesEmpty()
        {
            var restored = JsonSerializer.Deserialize<EntityId<PlainEntity>>("null");
            Assert.AreEqual(string.Empty, restored.Value);
        }

        [TestMethod]
        public void EntityId_Equality()
        {
            EntityId<PlainEntity> a = "same";
            EntityId<PlainEntity> b = "same";
            Assert.AreEqual(a, b);
            Assert.IsTrue(a == b);
        }
    }
}
