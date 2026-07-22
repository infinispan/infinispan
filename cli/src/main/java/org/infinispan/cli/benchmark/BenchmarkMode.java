package org.infinispan.cli.benchmark;

public enum BenchmarkMode {
   Throughput("Throughput (ops/time)"),
   AverageTime("Average time (time/op)"),
   SampleTime("Sampling time (time/op)");

   private final String longLabel;

   BenchmarkMode(String longLabel) {
      this.longLabel = longLabel;
   }

   public String longLabel() {
      return longLabel;
   }
}
