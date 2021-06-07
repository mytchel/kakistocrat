@0xd8ebb90861928a85;

struct Link {
  url @0 :UInt32;
  count @1 :UInt32;
}

struct Page {
  url @0 :Text;
  aliases @1 :List(Text);

  path @2 :Text;
  title @3 :Text;

  lastScanned @4 :UInt64;

  links @5 :List(Link);
}

struct Site {
  path @0 :Text;
  host @1 :Text;

  urls @2 :List(Text);

  pages @3 :List(Page);
}

struct IndexPart {
  path @0 :Text;
  sites @1 :List(Text);
}

struct ScoreNode {
  url @0 :Text;
  counter @1 :UInt32;
}

struct ScoreBlock {
  nodes @0 :List(ScoreNode);
}

struct ScoreWalk {
  url @0 :Text;
  hits @1 :UInt32;
}

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
    index @0 (sitePaths :List(Text), outputBase :Text) -> (outputs :List(IndexPart));
}

interface Merger {
    merge @0 (start :Text, end :Text, type :Text,
              indexPartPaths :List(Text),
              out :Text);
}

interface Scorer {
    registerScoreWorker @0 (worker :ScoreWorker);
    registerScoreReader @4 (reader :ScoreReader);

    score @1 (sitePaths :List(Text), seed :List(Text));

    getScore @2 (url :Text) -> (score :Float32);

    addWalks @3 (walks :List(ScoreWalk));
}

interface ScoreWorker {
    addSite @0 (sitePath :Text) -> (pageCount :UInt32);
    setSeed @1 (url :Text);

    setup @2 (k :UInt32, e :Float32, bias :UInt32, path :Text);

    iterate @3 ();
    iterateFinish @4 () -> (running :Bool);
    
    save @5 ();

    addWalks @6 (walks :List(ScoreWalk));
}

interface ScoreReader {
    load @0 (path :Text) -> (hosts :List(Text));
    getCounter @1 (url :Text) -> (counter :UInt32);
}

interface Searcher {
    search @0 (word :Text) -> (results: List(Text));
}

