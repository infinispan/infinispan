Tests require a running Elasticsearch instance, which is downloaded, started and stopped automatically by Maven plugins.

To manually start Elasticsearch and thus run tests from the IDE, run the ./start-elastic.sh script that uses docker.

Alternatively, after having built it at least once via Maven, you can launch the downloaded version from
 > cd target/elasticsearch0/bin
 > ./elasticsearch
