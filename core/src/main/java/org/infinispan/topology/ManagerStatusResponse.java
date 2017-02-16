package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.marshall.core.Ids;

/**
* @author Dan Berindei
* @since 7.1
*/
public class ManagerStatusResponse implements Serializable {
   private final Map<String, CacheStatusResponse> caches;
   private final boolean rebalancingEnabled;

   public ManagerStatusResponse(Map<String, CacheStatusResponse> caches, boolean rebalancingEnabled) {
      this.rebalancingEnabled = rebalancingEnabled;
      this.caches = caches;
   }

   public Map<String, CacheStatusResponse> getCaches() {
      return caches;
   }

   public boolean isRebalancingEnabled() {
      return rebalancingEnabled;
   }

   @Override
   public String toString() {
      return "ManagerStatusResponse{" +
            "caches=" + caches +
            ", rebalancingEnabled=" + rebalancingEnabled +
            '}';
   }

   public static class Externalizer extends InstanceReusingAdvancedExternalizer<ManagerStatusResponse> {
      @Override
      public void doWriteObject(ObjectOutput output, ManagerStatusResponse cacheStatusResponse) throws IOException {
         output.writeObject(cacheStatusResponse.caches);
         output.writeBoolean(cacheStatusResponse.rebalancingEnabled);
      }

      @Override
      public ManagerStatusResponse doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Map<String, CacheStatusResponse> caches = (Map<String, CacheStatusResponse>) unmarshaller.readObject();
         boolean rebalancingEnabled = unmarshaller.readBoolean();
         return new ManagerStatusResponse(caches, rebalancingEnabled);
      }

      @Override
      public Integer getId() {
         return Ids.MANAGER_STATUS_RESPONSE;
      }

      @Override
      public Set<Class<? extends ManagerStatusResponse>> getTypeClasses() {
         return Collections.<Class<? extends ManagerStatusResponse>>singleton(ManagerStatusResponse.class);
      }
   }
}
