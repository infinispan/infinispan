package org.infinispan.query.remote.json;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @since 9.4
 */
public abstract class JsonQueryResponse implements JsonSerialization {

   private final int hitCount;
   private final boolean hitCountExact;

   JsonQueryResponse(int hitCount, boolean hitCountExact) {
      this.hitCount = hitCount;
      this.hitCountExact = hitCountExact;
   }

   public int hitCount() {
      return hitCount;
   }

   public boolean hitCountExact() {
      return hitCountExact;
   }
}
