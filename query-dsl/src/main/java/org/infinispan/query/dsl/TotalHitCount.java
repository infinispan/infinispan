package org.infinispan.query.dsl;

public class TotalHitCount {

   public static final TotalHitCount EMPTY = new TotalHitCount(0, true);

   private final int value;
   private final boolean exact;

   public TotalHitCount(int value, boolean exact) {
      this.value = value;
      this.exact = exact;
   }

   /**
    * This returned value could be exact or a lower-bound of the exact value.
    * <p>
    * When the query is non-indexed, for performance reasons,
    * the hit count is not calculated and will return -1.
    *
    * @return the total hit count value
    * @see #isExact()
    */
   public int value() {
      return value;
   }

   /**
    * For efficiency reasons the computation of the hit count could be limited to some upper bound.
    * If the hit account accuracy is limited, the {@link #value()} here could be a lower-bound of the exact value,
    * and in this case this method will return {@code false}.
    *
    * @return whether the {@link #value()} is exact
    */
   public boolean isExact() {
      return exact;
   }
}
