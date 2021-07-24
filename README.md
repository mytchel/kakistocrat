# Web crawler, Indexer, Page Ranker and Searcher

Crawls the web starting with a list you give it.
Multi-process (but not multi-machine, running over nfs could work) crawling, indexing, and scoring.
Processes communicating over capnproto rpc.
With a simple http server for searching.
Pages are ranked with a bias towards ones on your initial list or close to those.

Written in c++.

Indexer and search initialy based off https://github.com/vkitchen/cocomel
Now completly re-written.

### Todo

* Better management so it can be left running by itself. Currently crawling, and indexing work for this.
  Scoring does not at all. And the search server doesn't reload when a new index is ready.
* Switch to something more sensible like zeromq rather than capnproto rpc.
* Distributed running. Using an object store like ceph?
* Rewrite in something other than c++.
* All the pages are stored. It would be cool for it to work as a personal archive. Even better if it
  supported version history of pages and rss for getting timely updates.
* Crawler should concatenate and compress pages to give to the indexer.
* Multiple user support.

### Requierments

* cmake
* c++
* zlib
* capnproto
* libcurl

* submodules:
** nlohmann
** spdlog

### Building & Running

Running is a bit of a process.

```

# Edit input files.
# You'll need to edit cmake to get it to not fail on not finding these
# as I store mine in a submodule.

# Edit the config.json to set limits that you want.

# Both are lists. Empty lines are ignored, as are lines starting with '#'
vi seed
vi blacklist

# Then build

mkdir build
cd build
cmake ..

# Then start running

# This manages everything. Ideally it shouldn't be doing much work itself.
# Run one of these
./main_capnp

# Then start up a couple of these. They will get sites from main and crawl them
# up to a limit in the config.
# They will write the pages and metadata to the filesystem.
# Once they have finished with a site they tell main. Main then reloads the metadata
# and adds new sites to be crawled.
# This process grows out as configured.
./crawler_capnp  # Run one or more of these

# Onces sites are crawled they will be given to the indexer's in batches.
# Start as many as you want.
# These index sites and build an index then dump that index when they hit
# the max memory from the config or run out of sites.
# These partial indexes are split (the number of splits is in the config) for the
# merger to join up.

# Think of the parts like this with part 1, 2, 3 able to be done in parallel
# or sequantially if there is not enough memory to do it all in one go.

# part 1: a b c d
# part 2: a b c d
# part 3: a b c d

./indexer_capnp  # Run one or more of these

# Then the mergers go through and join the parts by splits so you end up with 
# split a: with part 1, 2, 3
# split b: with part 1, 2, 3
# split c: with part 1, 2, 3
# split d: with part 1, 2, 3

# You can run as many of these as you want.
./merger_capnp

# Then to search use the search server which starts an http server at ':8000'.
# Run this once
# You will need to restart it if another merge happens otherwise it will have the old
# (possibly no junk) metadata.

# It loads each index split for each word and is not very efficient.
./search_capnp

# But you'll want page rankings.
# This part is messy.

# Run one scorer master. This connects to main and gets a list of sites.
./scorer_master_capnp
# It will wait a few seconds for scorer workers to connect and then start up with those.
# You will need to launch any scorer workers you want within 5s or so.
./scorer_worker_capnp &
./scorer_worker_capnp &
./scorer_worker_capnp &
./scorer_worker_capnp &

# They will go through and score everything.
# I recommend `ncpu - 1` scorer workers assuming there is no indexing happening while you score.

# Once the scores are made you can start running scorer readers.
# You will need the same number of readers as you had workers.
./scorer_reader_capnp &
./scorer_reader_capnp &
./scorer_reader_capnp &
./scorer_reader_capnp &

# Now try searching again and you should have scores.
```

