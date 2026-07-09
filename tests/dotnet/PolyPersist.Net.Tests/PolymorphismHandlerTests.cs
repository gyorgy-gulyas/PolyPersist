using System.Text.Json;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class PolymorphismHandlerTests
    {
        public abstract class Animal { public string Sound { get; set; } = ""; }
        public class Dog : Animal { public bool Barks { get; set; } = true; }
        public class Cat : Animal { public int Lives { get; set; } = 9; }
        public class NotAnAnimal { }

        [TestInitialize]
        public void Reset()
        {
            PolymorphismHandler.Clear();
            PolymorphismHandler.SetIgnoreUnrecognized(true);
        }

        [TestCleanup]
        public void Cleanup()
        {
            PolymorphismHandler.Clear();
            PolymorphismHandler.SetIgnoreUnrecognized(true);
        }

        [TestMethod]
        public void Register_And_GetDerivedTypes()
        {
            PolymorphismHandler.Register<Animal, Dog>();
            PolymorphismHandler.Register<Animal, Cat>();

            var derived = PolymorphismHandler.GetDerivedTypes(typeof(Animal)).ToList();
            Assert.AreEqual(2, derived.Count);
            CollectionAssert.Contains(derived.Select(d => d.Derived).ToList(), typeof(Dog));
            CollectionAssert.Contains(derived.Select(d => d.Derived).ToList(), typeof(Cat));
            Assert.AreEqual("Dog", derived.First(d => d.Derived == typeof(Dog)).Discriminator);
        }

        [TestMethod]
        public void GetDerivedTypes_Unregistered_ReturnsEmpty()
        {
            var derived = PolymorphismHandler.GetDerivedTypes(typeof(Animal)).ToList();
            Assert.AreEqual(0, derived.Count);
        }

        [TestMethod]
        public void Register_DuplicateDerived_Throws()
        {
            PolymorphismHandler.Register<Animal, Dog>();
            Assert.ThrowsException<InvalidOperationException>(
                () => PolymorphismHandler.Register<Animal, Dog>());
        }

        [TestMethod]
        public void Configure_NullOptions_Throws()
        {
            Assert.ThrowsException<ArgumentNullException>(
                () => PolymorphismHandler.Configure(null!));
        }

        [TestMethod]
        public void Configure_NoRegistrations_LeavesResolverNull()
        {
            var options = new JsonSerializerOptions();
            PolymorphismHandler.Configure(options);
            Assert.IsNull(options.TypeInfoResolver);
        }

        [TestMethod]
        public void Configure_WithRegistrations_RoundTripsPolymorphically()
        {
            PolymorphismHandler.Register<Animal, Dog>();
            PolymorphismHandler.Register<Animal, Cat>();

            var options = JsonOptionsProvider.Options();

            var animals = new List<Animal> { new Dog { Sound = "woof" }, new Cat { Sound = "meow" } };
            var json = JsonSerializer.Serialize(animals, options);

            StringAssert.Contains(json, "$type");
            StringAssert.Contains(json, "Dog");
            StringAssert.Contains(json, "Cat");

            var restored = JsonSerializer.Deserialize<List<Animal>>(json, options)!;
            Assert.IsInstanceOfType(restored[0], typeof(Dog));
            Assert.IsInstanceOfType(restored[1], typeof(Cat));
        }

        [TestMethod]
        public void Configure_CombinesWithExistingResolver()
        {
            PolymorphismHandler.Register<Animal, Dog>();

            var options = new JsonSerializerOptions
            {
                TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver()
            };
            PolymorphismHandler.Configure(options);
            // Combine branch exercised: resolver stays non-null after combining.
            Assert.IsNotNull(options.TypeInfoResolver);

            var json = JsonSerializer.Serialize<Animal>(new Dog { Sound = "woof" }, options);
            StringAssert.Contains(json, "woof");
        }

        [TestMethod]
        public void Clear_RemovesRegistrations()
        {
            PolymorphismHandler.Register<Animal, Dog>();
            PolymorphismHandler.Clear();
            Assert.AreEqual(0, PolymorphismHandler.GetDerivedTypes(typeof(Animal)).Count());
        }
    }
}
