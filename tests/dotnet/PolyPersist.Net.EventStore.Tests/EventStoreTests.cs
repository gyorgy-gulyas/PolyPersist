using System.Data.Common;
using Dapper;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.EventStore.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class EventStoreTests
    {
        private const int Any = -2;
        private const int NoStream = -1;

        private static async Task<IEventStore> NewStore(Func<string, Task<IEventStore>> factory)
            => await factory(TestMain.NewTableName());

        private static IEvent Ev(string type, string data, string metadata = null!)
            => new Event { eventType = type, data = data, metadata = metadata };

        private static List<IEvent> Evs(params IEvent[] e) => new(e);

        // ---- store basics ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StorageModel_Is_EventStore(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            Assert.AreEqual(IStore.StorageModels.EventStore, ((IStore)store).StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(((IStore)store).ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StreamExists_And_Version_ForNewStream(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();

            Assert.IsFalse(await store.StreamExists(stream));
            Assert.AreEqual(NoStream, await store.GetStreamVersion(stream));
        }

        // ---- append + read ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_Then_Read_Roundtrip(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();

            int last = await store.AppendToStream(stream, NoStream, Evs(
                Ev("Created", "{\"n\":1}"),
                Ev("Updated", "{\"n\":2}"),
                Ev("Closed", "{\"n\":3}")));

            Assert.AreEqual(2, last);                    // 0-based last version
            Assert.IsTrue(await store.StreamExists(stream));
            Assert.AreEqual(2, await store.GetStreamVersion(stream));

            var read = await store.ReadStream(stream, 0, -1);
            Assert.AreEqual(3, read.Count);
            Assert.AreEqual("Created", read[0].eventType);
            Assert.AreEqual(0, read[0].version);
            Assert.AreEqual("Updated", read[1].eventType);
            Assert.AreEqual(1, read[1].version);
            Assert.AreEqual("Closed", read[2].eventType);
            Assert.AreEqual(2, read[2].version);
            Assert.AreEqual(stream, read[0].streamId);
            Assert.AreEqual("{\"n\":1}", read[0].data);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_Assigns_EventId_And_Timestamp(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();

            await store.AppendToStream(stream, NoStream, Evs(Ev("E", "d")));
            var read = await store.ReadStream(stream, 0, -1);

            Assert.IsFalse(string.IsNullOrEmpty(read[0].eventId));
            Assert.AreNotEqual(default, read[0].timestamp);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_Preserves_Metadata(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();

            await store.AppendToStream(stream, NoStream, Evs(Ev("E", "d", "{\"user\":\"alice\"}")));
            var read = await store.ReadStream(stream, 0, -1);
            Assert.AreEqual("{\"user\":\"alice\"}", read[0].metadata);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_MultipleCalls_ContiguousVersions(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();

            int last0 = await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1")));
            Assert.AreEqual(0, last0);
            int last1 = await store.AppendToStream(stream, 0, Evs(Ev("B", "2"), Ev("C", "3")));
            Assert.AreEqual(2, last1);

            var read = await store.ReadStream(stream, 0, -1);
            CollectionAssert.AreEqual(new[] { 0, 1, 2 }, read.Select(e => e.version).ToArray());
            CollectionAssert.AreEqual(new[] { "A", "B", "C" }, read.Select(e => e.eventType).ToArray());
        }

        // ---- optimistic concurrency ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_NoStream_ToExistingStream_Throws(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1")));

            var ex = await Assert.ThrowsExceptionAsync<Exception>(
                () => store.AppendToStream(stream, NoStream, Evs(Ev("B", "2"))));
            Assert.IsTrue(ex.Message.Contains("already exists"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_WrongExpectedVersion_Throws(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"), Ev("B", "2"))); // last = 1

            var ex = await Assert.ThrowsExceptionAsync<Exception>(
                () => store.AppendToStream(stream, 0, Evs(Ev("C", "3")))); // expected 0, but is 1
            Assert.IsTrue(ex.Message.Contains("expected version"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_CorrectExpectedVersion_Ok(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1")));       // last = 0
            int last = await store.AppendToStream(stream, 0, Evs(Ev("B", "2")));   // expected 0 -> ok
            Assert.AreEqual(1, last);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Append_Any_AlwaysAppends(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, Any, Evs(Ev("A", "1")));
            int last = await store.AppendToStream(stream, Any, Evs(Ev("B", "2")));
            Assert.AreEqual(1, last);
            Assert.AreEqual(2, (await store.ReadStream(stream, 0, -1)).Count);
        }

        // ---- read variants ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ReadStream_FromVersion_Skips_Earlier(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"), Ev("B", "2"), Ev("C", "3")));

            var read = await store.ReadStream(stream, 1, -1);
            CollectionAssert.AreEqual(new[] { 1, 2 }, read.Select(e => e.version).ToArray());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ReadStream_MaxCount_Limits(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"), Ev("B", "2"), Ev("C", "3")));

            var read = await store.ReadStream(stream, 0, 2);
            Assert.AreEqual(2, read.Count);
            Assert.AreEqual(0, read[0].version);
            Assert.AreEqual(1, read[1].version);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ReadStream_Nonexistent_ReturnsEmpty(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var read = await store.ReadStream(TestMain.NewStreamId(), 0, -1);
            Assert.AreEqual(0, read.Count);
        }

        // ---- delete ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DeleteStream_Removes_Stream(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"), Ev("B", "2")));

            await store.DeleteStream(stream, Any);
            Assert.IsFalse(await store.StreamExists(stream));
            Assert.AreEqual(0, (await store.ReadStream(stream, 0, -1)).Count);
            Assert.AreEqual(NoStream, await store.GetStreamVersion(stream));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DeleteStream_WrongExpectedVersion_Throws(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"))); // last = 0

            var ex = await Assert.ThrowsExceptionAsync<Exception>(() => store.DeleteStream(stream, 5));
            Assert.IsTrue(ex.Message.Contains("expected version"));
        }

        // ---- isolation + model-vs-use ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Streams_Are_Isolated(Func<string, Task<IEventStore>> factory)
        {
            var store = await NewStore(factory);
            var s1 = TestMain.NewStreamId();
            var s2 = TestMain.NewStreamId();

            await store.AppendToStream(s1, NoStream, Evs(Ev("A", "1")));
            await store.AppendToStream(s2, NoStream, Evs(Ev("B", "2"), Ev("C", "3")));

            Assert.AreEqual(0, await store.GetStreamVersion(s1));
            Assert.AreEqual(1, await store.GetStreamVersion(s2));
            Assert.AreEqual(1, (await store.ReadStream(s1, 0, -1)).Count);
            Assert.AreEqual(2, (await store.ReadStream(s2, 0, -1)).Count);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DomainAndAudit_Streams_Coexist(Func<string, Task<IEventStore>> factory)
        {
            // one event store serves BOTH event sourcing (domain) and audit - just different streams.
            var store = await NewStore(factory);
            var order = "order-" + Guid.NewGuid().ToString("N");
            var audit = "audit-" + Guid.NewGuid().ToString("N");

            await store.AppendToStream(order, NoStream, Evs(Ev("OrderPlaced", "{}")));
            await store.AppendToStream(audit, NoStream, Evs(Ev("Login", "{}"), Ev("PasswordChanged", "{}")));

            Assert.AreEqual("OrderPlaced", (await store.ReadStream(order, 0, -1)).Single().eventType);
            Assert.AreEqual(2, (await store.ReadStream(audit, 0, -1)).Count);
        }

        // ---- escape hatch ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetUnderlyingImplementation_RawSql_Ok(Func<string, Task<IEventStore>> factory)
        {
            var table = TestMain.NewTableName();
            var store = await factory(table);
            var stream = TestMain.NewStreamId();
            await store.AppendToStream(stream, NoStream, Evs(Ev("A", "1"), Ev("B", "2")));

            var underlying = store.GetUnderlyingImplementation();
            Assert.IsNotNull(underlying);

            // The escape hatch is backend-specific: only the SQL backends expose a DbConnection.
            if (underlying is DbConnection conn)
            {
                await conn.OpenAsync();
                long count = await conn.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM \"{table}\" WHERE \"streamId\" = @s", new { s = stream });
                Assert.AreEqual(2L, count);
                conn.Dispose();
            }
        }
    }
}
