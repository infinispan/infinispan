package org.infinispan.multimap.impl.function.sortedset;

public enum SubsetType {
   INDEX, SCORE, LEX, OTHER;
   private static final SubsetType[] CACHED_VALUES = values();
   public static SubsetType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }
}
