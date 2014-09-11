package org.infinispan.topology;

import org.infinispan.partionhandling.impl.AvailabilityMode;
import org.infinispan.partionhandling.impl.AvailabilityStrategyContext;

import java.io.Serializable;

/**
* @author Dan Berindei
* @since 7.0
*/
public class CacheStatusResponse implements Serializable {
   private final CacheJoinInfo cacheJoinInfo;
   private final CacheTopology cacheTopology;
   private final CacheTopology stableTopology;
   private final AvailabilityMode availabilityMode;

   public CacheStatusResponse(CacheJoinInfo cacheJoinInfo, CacheTopology cacheTopology, CacheTopology stableTopology,
         AvailabilityMode availabilityMode) {
      this.cacheJoinInfo = cacheJoinInfo;
      this.cacheTopology = cacheTopology;
      this.stableTopology = stableTopology;
      this.availabilityMode = availabilityMode;
   }

   public CacheJoinInfo getCacheJoinInfo() {
      return cacheJoinInfo;
   }

   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   /**
    * @see org.infinispan.partionhandling.impl.AvailabilityStrategyContext#getStableTopology()
    */
   public CacheTopology getStableTopology() {
      return stableTopology;
   }

   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   @Override
   public String toString() {
      return "StatusResponse{" +
            "cacheJoinInfo=" + cacheJoinInfo +
            ", cacheTopology=" + cacheTopology +
            ", stableTopology=" + stableTopology +
            '}';
   }
}
