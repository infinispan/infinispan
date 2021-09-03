package org.infinispan.persistence.remote.upgrade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;

/**
 * Task to check for the remote store in a cache.
 *
 * @since 13.0
 */
public class CheckRemoteStoreTask implements Function<EmbeddedCacheManager, Boolean> {
   private final String cacheName;

   public CheckRemoteStoreTask(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   @SuppressWarnings("rawtypes")
   public Boolean apply(EmbeddedCacheManager embeddedCacheManager) {
      ComponentRegistry cr = embeddedCacheManager.getCache(cacheName).getAdvancedCache().getComponentRegistry();
      PersistenceManager persistenceManager = cr.getComponent(PersistenceManager.class);
      try {
         List<RemoteStore> stores = new ArrayList<>(persistenceManager.getStores(RemoteStore.class));
         return stores.size() == 1;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public static class Externalizer extends AbstractExternalizer<CheckRemoteStoreTask> {
      @Override
      public Set<Class<? extends CheckRemoteStoreTask>> getTypeClasses() {
         return Collections.singleton(CheckRemoteStoreTask.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CheckRemoteStoreTask task) throws IOException {
         output.writeObject(task.cacheName);
      }

      @Override
      public CheckRemoteStoreTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         return new CheckRemoteStoreTask(cacheName);
      }
   }
}
