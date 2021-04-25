@0xd8ebb90861928a85;

interface Master {
    registerCrawler @0 (crawler :Crawler);
    registerIndexer @1 (indexer :Indexer);
    registerMerger @2 (merger :Merger);
}

interface Crawler {
    crawl @0 (sitePath :Text, dataPath :Text,
              maxPages :UInt32);
}

interface Indexer {
    index @0 (sitePath :Text);
    flush @1 () -> (outputPaths :List(Text));
}

interface Merger {
    merge @0 (start :Text, end :Text,
              indexPartPaths :List(Text),
              wOut :Text, pOut :Text, tOut :Text);
}

