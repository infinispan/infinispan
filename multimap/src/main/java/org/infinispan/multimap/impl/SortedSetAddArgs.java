package org.infinispan.multimap.impl;

/**
 * Utility class to hold multiple options for sorted sets
 * @since 15.0
 */
public class SortedSetAddArgs {
   public static final String ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR = "addOnly and updateOnly can't both be true";
   public static final String ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR = "addOnly, updateGreaterScoresOnly and updateLessScoresOnly can't all be true";
   public boolean addOnly;
   public boolean updateOnly;
   public boolean updateLessScoresOnly;
   public boolean updateGreaterScoresOnly;
   public boolean returnChangedCount;
   public boolean replace;
   public boolean incr;


   private SortedSetAddArgs(SortedSetAddArgs.Builder builder) {
      this.addOnly = builder.addOnly;
      this.updateOnly = builder.updateOnly;
      this.updateLessScoresOnly = builder.updateLessScoresOnly;
      this.updateGreaterScoresOnly = builder.updateGreaterScoresOnly;
      this.returnChangedCount = builder.returnChangedCount;
      this.replace = builder.replace;
      this.incr = builder.incr;
   }
   public static Builder create() {
      return new Builder();
   }

   public static class Builder {
      private boolean addOnly;
      private boolean updateOnly;
      private boolean updateLessScoresOnly;
      private boolean updateGreaterScoresOnly;
      private boolean returnChangedCount;
      private boolean replace;
      private boolean incr;

      private Builder() {
      }

      public SortedSetAddArgs.Builder addOnly() {
         this.addOnly = true;
         return this;
      }

      public SortedSetAddArgs.Builder updateOnly() {
         this.updateOnly = true;
         return this;
      }

      public SortedSetAddArgs.Builder updateLessScoresOnly() {
         this.updateLessScoresOnly = true;
         return this;
      }

      public SortedSetAddArgs.Builder updateGreaterScoresOnly() {
         this.updateGreaterScoresOnly = true;
         return this;
      }

      public SortedSetAddArgs.Builder returnChangedCount() {
         this.returnChangedCount = true;
         return this;
      }

      public SortedSetAddArgs.Builder replace() {
         this.replace = true;
         return this;
      }

      public SortedSetAddArgs.Builder incr() {
         this.incr = true;
         return this;
      }

      public SortedSetAddArgs build(){
         // validate
         if (updateOnly && addOnly) {
            throw new IllegalStateException(ADD_AND_UPDATE_ONLY_INCOMPATIBLE_ERROR);
         }

         if ((addOnly && updateGreaterScoresOnly)
               || (addOnly && updateLessScoresOnly)
               || (updateGreaterScoresOnly && updateLessScoresOnly)
         ) {
            throw new IllegalStateException(ADD_AND_UPDATE_OPTIONS_INCOMPATIBLE_ERROR);
         }
         return new SortedSetAddArgs(this);
      }
   }
}
