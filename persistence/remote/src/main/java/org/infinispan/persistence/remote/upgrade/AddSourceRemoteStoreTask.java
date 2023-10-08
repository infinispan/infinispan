package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.SerializationUtils.fromJson;
import static org.infinispan.persistence.remote.upgrade.SerializationUtils.toJson;

import java.io.IOException;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Task to add a remote store to the cache.
 *
 * @since 13.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_ADD)
public class AddSourceRemoteStoreTask implements Function<EmbeddedCacheManager, Void> {
   private final String cacheName;
   private final RemoteStoreConfiguration storeConfiguration;

   public AddSourceRemoteStoreTask(String cacheName, RemoteStoreConfiguration storeConfiguration) {
      this.cacheName = cacheName;
      this.storeConfiguration = storeConfiguration;
   }

   @ProtoFactory
   public AddSourceRemoteStoreTask(String cacheName, String configJson) {
      this.cacheName = cacheName;
      try {
         this.storeConfiguration = fromJson(configJson);
      } catch (IOException e) {
         throw new MarshallingException("Unable to parse configuration json", e);
      }
   }

   @ProtoField(1)
   String getCacheName() {
      return cacheName;
   }

   @ProtoField(2)
   String getConfigJson() {
      return toJson(storeConfiguration);
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      ComponentRegistry cr = ComponentRegistry.of(embeddedCacheManager.getCache(cacheName));
      PersistenceManager persistenceManager = cr.getComponent(PersistenceManager.class);
      try {
         if (persistenceManager.getStores(RemoteStore.class).isEmpty()) {
             return CompletionStages.join(persistenceManager.addStore(storeConfiguration));
         }
         return null;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
