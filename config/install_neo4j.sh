#!/usr/bin/env bash

# === Install Neo4j in /graph directory ===

cd ~
cd /graph
wget http://neo4j.com/artifact.php?name=neo4j-community-2.3.1-unix.tar.gz
tar -xf artifact.php\?name\=neo4j-community-2.3.1-unix.tar.gz
rm artifact.php\?name\=neo4j-community-2.3.1-unix.tar.gz
mv neo4j-community-2.3.1/ neo4j/
NEO4J_HOME="/graph/neo4j/bin"
export NEO4J_HOME

neoauth neo4j neo4j my-p4ssword