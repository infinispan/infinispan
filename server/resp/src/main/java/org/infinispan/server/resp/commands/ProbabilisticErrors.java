package org.infinispan.server.resp.commands;

/**
 * Shared error message constants for probabilistic data structure commands (Bloom filters, Cuckoo filters).
 * These match the exact error strings returned by Redis module commands.
 */
public final class ProbabilisticErrors {
   public static final String ERR_NOT_FOUND = "ERR not found";
   public static final String ERR_CAPACITY = "ERR (capacity should be larger than 0)";
   public static final String ERR_UNKNOWN_ARGUMENT = "Unknown argument received";

   private ProbabilisticErrors() {
   }
}
