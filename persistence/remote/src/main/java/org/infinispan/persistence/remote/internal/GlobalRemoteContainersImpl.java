package org.infinispan.persistence.remote.internal;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfiguration;
import org.infinispan.persistence.remote.global.GlobalRemoteContainers;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @since 15.0
 **/
@Scope(Scopes.GLOBAL)
public class GlobalRemoteContainersImpl implements GlobalRemoteContainers {

   private static final org.infinispan.commons.logging.Log log = LogFactory.getLog(GlobalRemoteContainersImpl.class);

   @Inject
   GlobalConfiguration globalConfiguration;

   @Inject
   BlockingManager blockingManager;

   private final Map<String, CompletionStage<RemoteCacheManager>> remoteContainers = new ConcurrentHashMap<>();

   @Start
   public void start() {
      RemoteContainersConfiguration configuration = globalConfiguration.module(RemoteContainersConfiguration.class);
      if (configuration != null) {
         for (Map.Entry<String, RemoteContainerConfiguration> e : configuration.configurations().entrySet()) {
            remoteContainers.put(e.getKey(), CompletableFutures.completedNull());
         }
      }
   }

   @Stop
   public void stop() {
      remoteContainers.values().forEach(c -> c.thenApply(this::stop));
   }

   private Void stop(RemoteCacheManager rcm) {
      if (rcm != null) rcm.stop();
      return null;
   }

   private void remoteCacheManagerShutdown(String name) {
      remoteContainers.computeIfPresent(name, (key, prev) -> prev.thenApply(ignore -> null));
   }

   @Override
   public CompletionStage<RemoteCacheManager> cacheContainer(String name, Marshaller marshaller) {
      CompletionStage<RemoteCacheManager> cs = remoteContainers.computeIfPresent(name, (k, cf) ->
            cf.thenCompose(prev -> {
               if (prev != null && ((RefCountedRemoteCacheManager) prev).incrementReference()) {
                  return CompletableFuture.completedFuture(prev);
               }
               return createCacheManager(name, marshaller);
            }));

      if (cs == null) {
         return CompletableFuture.failedFuture(Log.CONFIG.unknownRemoteCacheManagerContainer(name));
      }

      return cs.thenApply(rcm -> {
         Marshaller inUse = rcm.getConfiguration().marshaller();
         if (!inUse.equals(marshaller)) throw Log.CONFIG.shouldUseSameMarshallerWithContainer(inUse, marshaller);
         return rcm;
      });
   }

   private CompletionStage<RemoteCacheManager> createCacheManager(String name, Marshaller marshaller) {
      RemoteContainersConfiguration configuration = globalConfiguration.module(RemoteContainersConfiguration.class);
      if (configuration != null) {
         RemoteContainerConfiguration rcc = configuration.configurations().get(name);

         if (rcc == null)
            return CompletableFuture.failedFuture(new IllegalStateException("No configuration defined for container " + name));

         ConfigurationBuilder builder = !rcc.uri().isEmpty()
               ? HotRodURI.create(rcc.uri()).toConfigurationBuilder()
               : new ConfigurationBuilder();

         builder.marshaller(marshaller);

         Properties propertiesToUse;
         Properties actualProperties = rcc.properties();
         if (!actualProperties.contains("blocking")) {
            // Need to make a copy to not change the actual configuration properties
            propertiesToUse = new Properties();
            propertiesToUse.putAll(actualProperties);
            // Make sure threads are marked as non blocking if user didn't specify
            propertiesToUse.put("blocking", "false");
         } else {
            propertiesToUse = actualProperties;
         }
         builder.withProperties(propertiesToUse);

         return blockingManager.supplyBlocking(() -> new RefCountedRemoteCacheManager(name, builder.build()), "RemoteContainer-create");
      }
      return CompletableFuture.failedFuture(new IllegalStateException("No remote container configuration defined"));
   }

   /**
    * Count the number of references to the {@link RemoteCacheManager}.
    * <p>
    * The shutdown only happens after the number of references reaches zero. Which also removes the class
    * from the {@link GlobalRemoteContainersImpl#remoteContainers}.
    */
   private class RefCountedRemoteCacheManager extends RemoteCacheManager {

      private final String name;
      private final AtomicInteger references;

      public RefCountedRemoteCacheManager(String name, Configuration configuration) {
         super(configuration);
         this.name = name;
         this.references = new AtomicInteger(1);
      }

      @Override
      public void stop() {
         if (references.decrementAndGet() == 0) {
            super.stop();
            remoteCacheManagerShutdown(name);
         }
      }

      public boolean incrementReference() {
         int curr = references.get();

         // Probably won't iterate (much), as the incrementReference is invoked from within the compute method.
         // This could conflict with a concurrent stop.
         while (curr > 0) {
            if (references.compareAndSet(curr, curr + 1)) {
               return true;
            }
            curr = references.get();
         }

         log.warnf("Remote cache manager '%s' was shutdown before acquiring, a new one will be created", name);
         return false;
      }
   }
}
