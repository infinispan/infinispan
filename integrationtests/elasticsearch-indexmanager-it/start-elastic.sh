#!/bin/bash

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=version.elasticsearch | grep -v '\[.*\]')

function waitForElastic()
{
  while ! curl http://localhost:9200  >/dev/null 2>&1; do sleep 1; done;
}

function startElastic()
{
  docker run --name elastic -d -p 9200:9200 -v "$PWD/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml" elasticsearch:$VERSION
  waitForElastic
}

function restartElastic()
{
  docker restart elastic
  waitForElastic
}

startElastic
restartElastic
