package org.infinispan.topology;

import java.io.Serializable;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
* @author Dan Berindei
* @since 7.0
*/
@ProtoTypeId(ProtoStreamTypeIds.CACHE_STATUS_RESPONSE)
public class CacheStatusResponse implements Serializable {

   private static final CacheStatusResponse EMPTY = new CacheStatusResponse(null, null, null, null, null);

   @ProtoField(1)
   final CacheJoinInfo cacheJoinInfo;

   @ProtoField(2)
   final CacheTopology cacheTopology;

   @ProtoField(3)
   final CacheTopology stableTopology;

   @ProtoField(4)
   final AvailabilityMode availabilityMode;

   @ProtoField(5)
   final List<Address> current;

   @ProtoFactory
   public CacheStatusResponse(CacheJoinInfo cacheJoinInfo, CacheTopology cacheTopology, CacheTopology stableTopology,
                              AvailabilityMode availabilityMode, List<Address> current) {
      this.cacheJoinInfo = cacheJoinInfo;
      this.cacheTopology = cacheTopology;
      this.stableTopology = stableTopology;
      this.availabilityMode = availabilityMode;
      this.current = current;
   }

   public static CacheStatusResponse empty() {
      return EMPTY;
   }

   public boolean isEmpty() {
      return cacheJoinInfo == null
            && cacheTopology == null
            && stableTopology == null
            && availabilityMode == null
            && current == null;
   }

   public CacheJoinInfo getCacheJoinInfo() {
      return cacheJoinInfo;
   }

   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   /**
    * @see org.infinispan.partitionhandling.impl.AvailabilityStrategyContext#getStableTopology()
    */
   public CacheTopology getStableTopology() {
      return stableTopology;
   }

   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   public List<Address> joinedMembers() {
      return current;
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
