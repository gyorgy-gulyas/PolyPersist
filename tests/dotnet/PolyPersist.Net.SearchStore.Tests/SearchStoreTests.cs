using PolyPersist.Net.Common;
namespace PolyPersist.Net.SearchStore.Tests
{
    /// <summary>
    /// Contract-level tests for every search backend, run data-driven over
    /// <see cref="TestMain.StoreInstances"/>. They assert only the PORTABLE surface — index lifecycle,
    /// id-addressed upsert/delete, term hit/miss, from/size paging, and the two search modes
    /// (FullText = whole-word; Fuzzy = substring OR typo). Engine-specific behaviour (exact relevance
    /// score, tokenisation/stemming, precise fuzzy distance) is intentionally NOT asserted here; that
    /// lives behind GetUnderlyingImplementation. Queries therefore use single, distinct, non-stopword
    /// terms so every engine agrees on hit vs. miss.
    /// </summary>
    [TestClass]
    public class SearchStoreTests
    {
        private static async Task<(ISearchStore store, ISearchIndex<Article> index, string name)> NewIndex(
            Func<Task<ISearchStore>> factory)
        {
            var store = await factory();
            var name = TestMain.NewIndexName();
            var index = await store.CreateIndex<Article>(name);
            return (store, index, name);
        }

        private static Article Doc(string id, string title, string body) => new() { id = id, Title = title, Body = body };

        private static List<string> Ids(IList<Article> hits) => hits.Select(d => d.id).ToList();

        // ---- index lifecycle ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task CreateIndex_Then_IsIndexExists_True(Func<Task<ISearchStore>> factory)
        {
            var (store, index, name) = await NewIndex(factory);

            Assert.IsTrue(await store.IsIndexExists(name));
            Assert.AreEqual(name, index.Name);
            Assert.AreSame(store, index.ParentStore);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task CreateIndex_Twice_Throws(Func<Task<ISearchStore>> factory)
        {
            var (store, _, name) = await NewIndex(factory);
            await Assert.ThrowsExceptionAsync<DuplicateKeyException>(() => store.CreateIndex<Article>(name));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task GetIndexByName_Missing_Throws(Func<Task<ISearchStore>> factory)
        {
            var store = await factory();
            await Assert.ThrowsExceptionAsync<NotFoundException>(() => store.GetIndexByName<Article>(TestMain.NewIndexName()));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task GetIndexByName_Existing_Ok(Func<Task<ISearchStore>> factory)
        {
            var (store, _, name) = await NewIndex(factory);
            var again = await store.GetIndexByName<Article>(name);
            Assert.AreEqual(name, again.Name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task DropIndex_Then_IsIndexExists_False(Func<Task<ISearchStore>> factory)
        {
            var (store, _, name) = await NewIndex(factory);
            await store.DropIndex(name);
            Assert.IsFalse(await store.IsIndexExists(name));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task DropIndex_Missing_Throws(Func<Task<ISearchStore>> factory)
        {
            var store = await factory();
            await Assert.ThrowsExceptionAsync<NotFoundException>(() => store.DropIndex(TestMain.NewIndexName()));
        }

        // ---- index + full-text search ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Index_Then_Search_FindsMatchingDocOnly(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "quantum", "physics lecture"));
            await index.Index(Doc("2", "volcano", "geology lecture"));

            var hits = await index.Search("quantum", SearchMode.FullText, 0, 10);
            CollectionAssert.AreEquivalent(new[] { "1" }, Ids(hits).ToArray());

            var other = await index.Search("volcano", SearchMode.FullText, 0, 10);
            CollectionAssert.AreEquivalent(new[] { "2" }, Ids(other).ToArray());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task IndexBatch_Then_Search_FindsAllSharingTerm(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.IndexBatch(new List<Article>
            {
                Doc("a", "report", "common summary"),
                Doc("b", "memo",   "common notice"),
                Doc("c", "letter", "common greeting"),
                Doc("d", "unrelated", "solitary text"),
            });

            var hits = await index.Search("common", SearchMode.FullText, 0, 100);
            CollectionAssert.AreEquivalent(new[] { "a", "b", "c" }, Ids(hits).ToArray());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Delete_Then_Search_Gone(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "quantum", "physics"));
            Assert.AreEqual(1, (await index.Search("quantum", SearchMode.FullText, 0, 10)).Count);

            await index.Delete("1");
            Assert.AreEqual(0, (await index.Search("quantum", SearchMode.FullText, 0, 10)).Count);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Index_Upsert_SameId_Replaces(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "alpha", "alpha body"));
            await index.Index(Doc("1", "omega", "omega body")); // same id -> overwrite

            Assert.AreEqual(0, (await index.Search("alpha", SearchMode.FullText, 0, 10)).Count);
            var hits = await index.Search("omega", SearchMode.FullText, 0, 10);
            CollectionAssert.AreEquivalent(new[] { "1" }, Ids(hits).ToArray());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Search_NoMatch_Empty(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "quantum", "physics"));

            var hits = await index.Search("nonexistentword", SearchMode.FullText, 0, 10);
            Assert.AreEqual(0, hits.Count);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Search_Paging_FromSize(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            // Five docs all containing the term, with distinct term-frequency so each engine has a
            // deterministic (stable) order to page over; we assert paging set-semantics, not order.
            var docs = Enumerable.Range(0, 5)
                .Select(i => Doc(i.ToString(), "doc", string.Join(' ', Enumerable.Repeat("common", i + 1))))
                .ToList();
            await index.IndexBatch(docs);

            var all = await index.Search("common", SearchMode.FullText, 0, 100);
            Assert.AreEqual(5, all.Count);
            CollectionAssert.AreEquivalent(new[] { "0", "1", "2", "3", "4" }, Ids(all).ToArray());

            var page1 = Ids(await index.Search("common", SearchMode.FullText, 0, 2));
            var page2 = Ids(await index.Search("common", SearchMode.FullText, 2, 2));
            var page3 = Ids(await index.Search("common", SearchMode.FullText, 4, 2));

            Assert.AreEqual(2, page1.Count);
            Assert.AreEqual(2, page2.Count);
            Assert.AreEqual(1, page3.Count);
            // Pages are disjoint and together cover all five.
            Assert.AreEqual(5, page1.Concat(page2).Concat(page3).Distinct().Count());
        }

        // ---- fuzzy search (substring + typo tolerance) ----

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Fuzzy_MatchesSubstringFragment(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "mountain", "peak"));
            await index.Index(Doc("2", "elephant", "animal"));

            // "ount" is a fragment inside "mountain" but not a whole word.
            var full = Ids(await index.Search("ount", SearchMode.FullText, 0, 10));
            CollectionAssert.DoesNotContain(full, "1");

            var fuzzy = Ids(await index.Search("ount", SearchMode.Fuzzy, 0, 10));
            CollectionAssert.Contains(fuzzy, "1");
            CollectionAssert.DoesNotContain(fuzzy, "2");
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain))]
        public async Task Fuzzy_ToleratesTypo(Func<Task<ISearchStore>> factory)
        {
            var (_, index, _) = await NewIndex(factory);
            await index.Index(Doc("1", "mountain", "peak"));
            await index.Index(Doc("2", "elephant", "animal"));

            // "mountian" is a typo of "mountain" (small edit distance), not an exact term.
            var full = Ids(await index.Search("mountian", SearchMode.FullText, 0, 10));
            CollectionAssert.DoesNotContain(full, "1");

            var fuzzy = Ids(await index.Search("mountian", SearchMode.Fuzzy, 0, 10));
            CollectionAssert.Contains(fuzzy, "1");
            CollectionAssert.DoesNotContain(fuzzy, "2");
        }
    }
}
