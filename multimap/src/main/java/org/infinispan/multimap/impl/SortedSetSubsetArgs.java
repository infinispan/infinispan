package org.infinispan.multimap.impl;

/**
 * Utility class to hold subset operation arguments
 * @since 15.0
 */
public class SortedSetSubsetArgs<T> {
   // reversed
   private final boolean isRev;
   private final T start;
   private final T stop;
   private final boolean includeStart;
   private final boolean includeStop;
   private Long offset;
   private Long count;

   private SortedSetSubsetArgs(SortedSetSubsetArgs.Builder<T> builder) {
      this.isRev = builder.isRev;
      this.start = builder.min;
      this.stop = builder.max;
      this.includeStart = builder.includeMin;
      this.includeStop = builder.includeMax;
      this.offset = builder.offset;
      this.count = builder.count;
   }

   public static Builder create() {
      return new Builder<>();
   }

   public static class Builder<T> {
      private boolean isRev;
      private T min;
      private T max;
      private boolean includeMin;
      private boolean includeMax;
      private Long offset;
      private Long count;

      private Builder() {
      }

      public SortedSetSubsetArgs.Builder<T> start(T start) {
         this.min = start;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> stop(T stop) {
         this.max = stop;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> includeStart(boolean include) {
         this.includeMin = include;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> includeStop(boolean include) {
         this.includeMax = include;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> isRev(boolean isRev) {
         this.isRev = isRev;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> offset(Long offset) {
         this.offset = offset;
         return this;
      }

      public SortedSetSubsetArgs.Builder<T> count(Long count) {
         this.count = count;
         return this;
      }

      public SortedSetSubsetArgs<T> build(){
         return new SortedSetSubsetArgs<>(this);
      }
   }

   public boolean isRev() {
      return isRev;
   }

   public T getStart() {
      return start;
   }

   public T getStop() {
      return stop;
   }

   public boolean isIncludeStart() {
      return includeStart;
   }

   public boolean isIncludeStop() {
      return includeStop;
   }

   public Long getCount() {
      return count;
   }

   public Long getOffset() {
      return offset;
   }

   @Override
   public String toString() {
      return "SortedSetSubsetArgs{" + "isRev=" + isRev + ", start=" + start + ", stop=" + stop + ", includeStart="
            + includeStart + ", includeStop=" + includeStop + ", offset=" + offset + ", count=" + count + '}';
   }
}
