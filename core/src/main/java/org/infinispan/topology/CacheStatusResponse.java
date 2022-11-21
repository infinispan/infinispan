package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.Ids;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;

/**
* @author Dan Berindei
* @since 7.0
*/
public class CacheStatusResponse implements Serializable {
   private final CacheJoinInfo cacheJoinInfo;
   private final CacheTopology cacheTopology;
   private final CacheTopology stableTopology;
   private final AvailabilityMode availabilityMode;
   private final List<Address> current;

   public CacheStatusResponse(CacheJoinInfo cacheJoinInfo, CacheTopology cacheTopology, CacheTopology stableTopology,
                              AvailabilityMode availabilityMode, List<Address> current) {
      this.cacheJoinInfo = cacheJoinInfo;
      this.cacheTopology = cacheTopology;
      this.stableTopology = stableTopology;
      this.availabilityMode = availabilityMode;
      this.current = current;
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

   public static class Externalizer extends AbstractExternalizer<CacheStatusResponse> {
      @Override
      public void writeObject(ObjectOutput output, CacheStatusResponse cacheStatusResponse) throws IOException {
         output.writeObject(cacheStatusResponse.cacheJoinInfo);
         output.writeObject(cacheStatusResponse.cacheTopology);
         output.writeObject(cacheStatusResponse.stableTopology);
         output.writeObject(cacheStatusResponse.availabilityMode);
         MarshallUtil.marshallCollection(cacheStatusResponse.current, output);
      }

      @Override
      public CacheStatusResponse readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         CacheJoinInfo cacheJoinInfo = (CacheJoinInfo) unmarshaller.readObject();
         CacheTopology cacheTopology = (CacheTopology) unmarshaller.readObject();
         CacheTopology stableTopology = (CacheTopology) unmarshaller.readObject();
         AvailabilityMode availabilityMode = (AvailabilityMode) unmarshaller.readObject();
         List<Address> current = MarshallUtil.unmarshallCollection(unmarshaller, ArrayList::new);
         return new CacheStatusResponse(cacheJoinInfo, cacheTopology, stableTopology, availabilityMode, current);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_STATUS_RESPONSE;
      }

      @Override
      public Set<Class<? extends CacheStatusResponse>> getTypeClasses() {
         return Collections.<Class<? extends CacheStatusResponse>>singleton(CacheStatusResponse.class);
      }
   }
}
