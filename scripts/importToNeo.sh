#!/usr/bin/env bash

cd /graph/neo4j/data/import

$NEO4J_HOME/neo4j-import --into /graph/neo4j/data/graph.db \
--skip-duplicate-nodes true --skip-bad-relationships true \
--ignore-empty-strings true \
--bad-tolerance 9000000 \
--processors $(nproc) \
--id-type string \
--nodes:ARTIST hdr-artist.csv,$(echo $(ls art*) | tr ' ' ,)  \
--nodes:SONG   hdr-song.csv,$(echo $(ls song*) | tr ' ' ,)  \
--nodes:ALBUM  hdr-album.csv,$(echo $(ls album*) | tr ' ' ,)  \
--nodes:YEAR   hdr-year.csv,$(echo $(ls year*) | tr ' ' ,)  \
--nodes:TAG    hdr-tag.csv,$(echo $(ls tag*) | tr ' ' ,)  \
--relationships:SIMILAR_TO  hdr-sim-a.csv,$(echo $(ls sim-a*) | tr ' ' ,)  \
--relationships:PERFORMS    hdr-perf.csv,$(echo $(ls perf*) | tr ' ' ,)  \
--relationships:HAS_ALBUM   hdr-has-album.csv,$(echo $(ls has-album*) | tr ' ' ,)  \
--relationships:HAS_TAG     hdr-a-has-tag.csv,$(echo $(ls a-has-tag*) | tr ' ' ,)  \
--relationships:IN_ALBUM    hdr-in-album.csv,$(echo $(ls in-album*) | tr ' ' ,)  \
--relationships:SIMILAR_TO  hdr-sim-s.csv,$(echo $(ls sim-s*) | tr ' ' ,)  \
--relationships:HAS_TAG     hdr-s-has-tag.csv,$(echo $(ls s-has-tag*) | tr ' ' ,)  \
--relationships:RELEASED_ON hdr-release.csv,$(echo $(ls release*) | tr ' ' ,) 