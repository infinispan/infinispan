package org.infinispan.multimap.impl.function.sortedset;

public enum SortedSetOperationType {
   INDEX, SCORE, LEX, OTHER;
   private static final SortedSetOperationType[] CACHED_VALUES = values();
   public static SortedSetOperationType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }
}
