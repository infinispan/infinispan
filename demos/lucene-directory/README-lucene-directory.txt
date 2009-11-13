This is an interactive demo of the Lucene Directory.
Run several copies of the demo, they should cluster automatically and see
from the commandline how you can index text on one node and search for it
on the others.

To build the demo, do:

mvn package appassembler:assemble

this should download dependencies and produce proper scripts to run it in target/assembly 