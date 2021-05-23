@0xd8ebb90861928a85;

interface Master {
    registerCrawler @0 (crawler :Crawler);

    registerIndexer @1 (indexer :Indexer);
    registerMerger @2 (merger :Merger);

    registerSearcher @3 (searcher :Searcher);

    registerScorer @4 (scorer :Scorer);

    getScore @5 (url :Text) -> (score :Float32);

    getPageInfo @6 (url :Text) -> (score :Float32, title :Text, path :Text);

    search @7 (query :Text) -> (results: List(Text));
}

interface Crawler {
    crawl @0 (sitePath :Text, dataPath :Text,
              maxPages :UInt32);

    canCrawl @1 () -> (haveSpace :Bool);
}

interface Indexer {
    index @0 (sitePaths :List(Text), outputBase :Text) -> (outputPaths :List(Text));
}

interface Merger {
    merge @0 (start :Text, end :Text,
              indexPartPaths :List(Text),
              wOut :Text, pOut :Text, tOut :Text);
}

interface Scorer {
    registerScorerWorker @0 (worker :ScorerWorker);

    score @1 (sitePaths :List(Text));

    getScore @2 (url :Text) -> (score :Float32);

    addWalk @3 (site :Text, url :Text, hits :UInt32) -> (found :Bool);
}

interface ScorerWorker {
    addSite @0 (sitePath :Text) -> (pageCount :UInt32);
    getCounter @1 (url :Text) -> (counter :UInt32);

    setup @2 (k :UInt32, e :Float32);
    iterate @3 ();
    iterateFinish @4 () -> (running :Bool);
    addWalk @5 (site :Text, url :Text, hits :UInt32) -> (found :Bool);
}

interface Searcher {
    search @0 (word :Text) -> (results: List(Text));
}

