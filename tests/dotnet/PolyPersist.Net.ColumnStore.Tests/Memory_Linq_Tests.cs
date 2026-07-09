using PolyPersist;
using PolyPersist.Net.ColumnStore.Memory;
using PolyPersist.Net.Extensions;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace PolyPersist.Net.ColumnStore.Tests
{
    // Memory-only LINQ tests. They exercise the Memory_ExpressionVisitor translation
    // branches (unsupported operators/projections, Contains, ThenBy, boolean combinators)
    // that the parameterized suite does not reach. They construct the in-memory store
    // directly, so they need no Docker.
    [TestClass]
    [DoNotParallelize]
    public class Memory_Linq_Tests
    {
        private static async Task<IColumnTable<SampleRow>> _seededTable()
        {
            IColumnStore store = new Memory_ColumnStore("");
            var table = await store.CreateTable<SampleRow>("linqtable");
            await table.Insert(new SampleRow { PartitionKey = "pk", id = "q1", str_value = "A", int_value = 10 });
            await table.Insert(new SampleRow { PartitionKey = "pk", id = "q2", str_value = "B", int_value = 20 });
            await table.Insert(new SampleRow { PartitionKey = "pk2", id = "q3", str_value = "C", int_value = 30 });
            return table;
        }

        [TestMethod]
        public async Task Where_ConstantOnLeft_NotSupported()
        {
            var table = await _seededTable();

            var ex = Assert.ThrowsException<NotSupportedException>(() =>
                table.AsQueryable().Where(r => "A" == r.str_value).ToList());
            Assert.IsTrue(ex.Message.Contains("Left side"));
        }

        [TestMethod]
        public async Task Where_UnsupportedRightSide_NotSupported()
        {
            var table = await _seededTable();

            var ex = Assert.ThrowsException<NotSupportedException>(() =>
                table.AsQueryable().Where(r => r.int_value == r.int_value + 1).ToList());
            Assert.IsTrue(ex.Message.Contains("not supported"));
        }

        [TestMethod]
        public async Task Where_AndAlso_OrElse_OK()
        {
            var table = await _seededTable();

            var andList = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "pk" && r.int_value == 10)
                .ToList();
            Assert.AreEqual(1, andList.Count);
            Assert.AreEqual("q1", andList[0].id);

            var orList = table
                .AsQueryable()
                .Where(r => r.str_value == "A" || r.int_value == 20)
                .ToList();
            Assert.AreEqual(2, orList.Count);
        }

        [TestMethod]
        public async Task Select_UnsupportedProjection_NotSupported()
        {
            var table = await _seededTable();

            var ex = Assert.ThrowsException<NotSupportedException>(() =>
                table.AsQueryable().Select(r => r.int_value + 1).ToList());
            Assert.IsTrue(ex.Message.Contains("Select projection"));
        }

        [TestMethod]
        public async Task Where_Contains_OK()
        {
            var table = await _seededTable();

            var ids = new[] { "q1", "q2" };
            var list = table
                .AsQueryable()
                .Where(r => ids.Contains(r.id))
                .ToList();

            Assert.AreEqual(2, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));
            Assert.IsNotNull(list.First(r => r.id == "q2"));
        }

        [TestMethod]
        public async Task OrderBy_ThenBy_OK()
        {
            var table = await _seededTable();

            var asc = table
                .AsQueryable()
                .OrderBy(r => r.str_value)
                .ThenBy(r => r.id)
                .ToList();
            Assert.AreEqual(3, asc.Count);
            Assert.AreEqual("q1", asc.First().id);

            var desc = table
                .AsQueryable()
                .OrderBy(r => r.str_value)
                .ThenByDescending(r => r.id)
                .ToList();
            Assert.AreEqual(3, desc.Count);
        }

        [TestMethod]
        public async Task OrderBy_NonMember_NotSupported()
        {
            var table = await _seededTable();

            var ex = Assert.ThrowsException<NotSupportedException>(() =>
                table.AsQueryable().OrderBy(r => r.int_value + 1).ToList());
            Assert.IsTrue(ex.Message.Contains("member expressions"));
        }
    }
}
