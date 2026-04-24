package org.infinispan.server.resp.commands;

/**
 * Shared error message constants for probabilistic data structure commands
 * (Bloom filters, Cuckoo filters, Count-Min Sketch).
 * These match the exact error strings returned by Redis module commands.
 */
public final class ProbabilisticErrors {
   public static final String ERR_NOT_FOUND = "ERR not found";
   public static final String ERR_CAPACITY = "ERR (capacity should be larger than 0)";
   public static final String ERR_UNKNOWN_ARGUMENT = "Unknown argument received";

   // CMS errors use a different prefix format than BF/CF
   public static final String CMS_KEY_NOT_FOUND = "CMS: key does not exist";
   public static final String CMS_KEY_EXISTS = "CMS: key already exists";
   public static final String CMS_CANNOT_PARSE = "CMS: Cannot parse number";
   public static final String CMS_NEGATIVE_NUMBER = "CMS: Number cannot be negative";
   public static final String CMS_INVALID_WIDTH = "CMS: invalid width";
   public static final String CMS_INVALID_DEPTH = "CMS: invalid depth";
   public static final String CMS_INVALID_OVERESTIMATION = "CMS: invalid overestimation value";
   public static final String CMS_INVALID_PROB = "CMS: invalid prob value";
   public static final String CMS_WIDTH_DEPTH_MISMATCH = "CMS: width/depth is not equal";
   public static final String CMS_INVALID_NUMKEYS = "CMS: invalid numkeys";
   public static final String CMS_NUMKEYS_POSITIVE = "CMS: Number of keys must be positive";
   public static final String CMS_WRONG_NUM_KEYS = "CMS: wrong number of keys";
   public static final String CMS_INVALID_WEIGHT = "CMS: invalid weight value";

   private ProbabilisticErrors() {
   }
}
