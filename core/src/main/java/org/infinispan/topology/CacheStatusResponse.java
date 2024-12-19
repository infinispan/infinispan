package org.infinispan.topology;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
* @author Dan Berindei
* @since 7.0
*/
@ProtoTypeId(ProtoStreamTypeIds.CACHE_STATUS_RESPONSE)
public class CacheStatusResponse implements Serializable {

   private static final CacheStatusResponse EMPTY = new CacheStatusResponse(null, null, null, null, (List<Address>) null);

   @ProtoField(1)
   final CacheJoinInfo cacheJoinInfo;

   @ProtoField(2)
   final CacheTopology cacheTopology;

   @ProtoField(3)
   final CacheTopology stableTopology;

   @ProtoField(4)
   final AvailabilityMode availabilityMode;

   final List<Address> current;

   public CacheStatusResponse(CacheJoinInfo cacheJoinInfo, CacheTopology cacheTopology, CacheTopology stableTopology,
                              AvailabilityMode availabilityMode, List<Address> current) {
      this.cacheJoinInfo = cacheJoinInfo;
      this.cacheTopology = cacheTopology;
      this.stableTopology = stableTopology;
      this.availabilityMode = availabilityMode;
      this.current = current;
   }

   @ProtoFactory
   static CacheStatusResponse create(CacheJoinInfo cacheJoinInfo, CacheTopology cacheTopology, CacheTopology stableTopology,
                              AvailabilityMode availabilityMode, List<JGroupsAddress> current) {
      return new CacheStatusResponse(cacheJoinInfo, cacheTopology, stableTopology, availabilityMode, (List<Address>)(List<?>) current);
   }

   @ProtoField(number = 5, collectionImplementation = ArrayList.class)
   List<JGroupsAddress> getCurrent() {
      return (List<JGroupsAddress>)(List<?>) current;
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
