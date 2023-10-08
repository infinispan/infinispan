package org.infinispan.persistence.remote.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Task to check for the remote store in a cache.
 *
 * @since 13.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_CHECK)
public class CheckRemoteStoreTask implements Function<EmbeddedCacheManager, Boolean> {

   @ProtoField(1)
   final String cacheName;

   @ProtoFactory
   public CheckRemoteStoreTask(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   @SuppressWarnings("rawtypes")
   public Boolean apply(EmbeddedCacheManager embeddedCacheManager) {
      PersistenceManager persistenceManager = ComponentRegistry.componentOf(embeddedCacheManager.getCache(cacheName), PersistenceManager.class);
      try {
         List<RemoteStore> stores = new ArrayList<>(persistenceManager.getStores(RemoteStore.class));
         return stores.size() == 1;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
