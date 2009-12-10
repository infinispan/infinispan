This is an interactive demo of the Lucene Directory implementation based on Infinispan.

Run more copies of the demo, they should cluster automatically.
Using a simple commandline interface you can play with it adding terms to the index and search from them.

See how different nodes interact and share the same index, you can add terms on one node and search from it on another.
The provided configuration is meant to work in most environment, for best performance

To build the demo, run:

mvn package appassembler:assemble

This command requires Maven2 and will download dependencies and produce scripts to run the demo in

   ./target/assembly/bin

