@0xd8ebb90861928a85;

interface Master {
    registerCrawler @0 (crawler :Crawler);
    registerIndexer @1 (indexer :Indexer);
    registerMerger @2 (merger :Merger);
    registerSearcher @3 (searcher :Searcher);
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

interface Searcher {
    search @0 (query :Text) -> (results: List(Text));
}

