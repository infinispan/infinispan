package org.infinispan.persistence.remote.upgrade;

import static org.infinispan.persistence.remote.upgrade.SerializationUtils.fromJson;
import static org.infinispan.persistence.remote.upgrade.SerializationUtils.toJson;

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
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Task to a remote store to the cache.
 *
 * @since 13.0
 */
public class AddSourceRemoteStoreTask implements Function<EmbeddedCacheManager, Void> {
   private final String cacheName;
   private final RemoteStoreConfiguration storeConfiguration;

   public AddSourceRemoteStoreTask(String cacheName, RemoteStoreConfiguration storeConfiguration) {
      this.cacheName = cacheName;
      this.storeConfiguration = storeConfiguration;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      ComponentRegistry cr = embeddedCacheManager.getCache(cacheName).getAdvancedCache().getComponentRegistry();
      PersistenceManager persistenceManager = cr.getComponent(PersistenceManager.class);
      try {
         return CompletionStages.join(persistenceManager.addStore(storeConfiguration));
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public static class Externalizer extends AbstractExternalizer<AddSourceRemoteStoreTask> {
      @Override
      public Set<Class<? extends AddSourceRemoteStoreTask>> getTypeClasses() {
         return Collections.singleton(AddSourceRemoteStoreTask.class);
      }

      @Override
      public void writeObject(ObjectOutput output, AddSourceRemoteStoreTask task) throws IOException {
         output.writeObject(task.cacheName);
         output.writeObject(toJson(task.storeConfiguration));
      }

      @Override
      public AddSourceRemoteStoreTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         String config = (String) input.readObject();
         return new AddSourceRemoteStoreTask(cacheName, fromJson(config));
      }
   }
}
