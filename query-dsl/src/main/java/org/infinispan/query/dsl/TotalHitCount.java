package org.infinispan.query.dsl;

import org.infinispan.commons.api.query.HitCount;

public class TotalHitCount implements HitCount {

   public static final TotalHitCount EMPTY = new TotalHitCount(0, true);

   private final int value;
   private final boolean exact;

   public TotalHitCount(int value, boolean exact) {
      this.value = value;
      this.exact = exact;
   }

   @Override
   public int value() {
      return value;
   }

   @Override
   public boolean isExact() {
      return exact;
   }
}
