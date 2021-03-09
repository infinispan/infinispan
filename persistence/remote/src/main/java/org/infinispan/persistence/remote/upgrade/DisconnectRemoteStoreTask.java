package org.infinispan.persistence.remote.upgrade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

/**
 * Cluster task to remove the remote store from a set a caches
 *
 * @since 12.1
 */
public class DisconnectRemoteStoreTask implements Function<EmbeddedCacheManager, Void> {

   private static final Log log = LogFactory.getLog(DisconnectRemoteStoreTask.class, Log.class);

   private final String cacheName;

   public DisconnectRemoteStoreTask(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      ComponentRegistry cr = embeddedCacheManager.getCache(cacheName).getAdvancedCache().getComponentRegistry();
      PersistenceManager persistenceManager = cr.getComponent(PersistenceManager.class);
      try {
         log.debugf("Disconnecting source for cache {}", cacheName);
         return CompletionStages.join(persistenceManager.disableStore(RemoteStore.class.getName()));
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public static class Externalizer extends AbstractExternalizer<DisconnectRemoteStoreTask> {

      @Override
      public Set<Class<? extends DisconnectRemoteStoreTask>> getTypeClasses() {
         return Collections.singleton(DisconnectRemoteStoreTask.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DisconnectRemoteStoreTask task) throws IOException {
         output.writeObject(task.cacheName);
      }

      @Override
      public DisconnectRemoteStoreTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         return new DisconnectRemoteStoreTask(cacheName);
      }
   }

}
