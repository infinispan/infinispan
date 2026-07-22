package org.infinispan.cli.benchmark;

public interface BenchmarkTask {
   void setup();

   void run();

   void teardown();

   String name();
}
