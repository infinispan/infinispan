package org.infinispan.persistence.remote.upgrade;

import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.LogFactory;

/**
 * Cluster task to remove the remote store from a set a caches
 *
 * @since 12.1
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_DISCONNECT)
public class DisconnectRemoteStoreTask implements Function<EmbeddedCacheManager, Void> {

   private static final Log log = LogFactory.getLog(DisconnectRemoteStoreTask.class, Log.class);

   @ProtoField(1)
   final String cacheName;

   @ProtoFactory
   public DisconnectRemoteStoreTask(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      PersistenceManager persistenceManager = ComponentRegistry.componentOf(embeddedCacheManager.getCache(cacheName), PersistenceManager.class);
      try {
         log.debugf("Disconnecting source for cache {}", cacheName);
         return CompletionStages.join(persistenceManager.disableStore(RemoteStore.class.getName()));
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
