package org.infinispan.persistence.remote.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfiguration;
import org.infinispan.persistence.remote.global.GlobalRemoteContainers;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @since 15.0
 **/
@Scope(Scopes.GLOBAL)
public class GlobalRemoteContainersImpl implements GlobalRemoteContainers {

   @Inject
   GlobalConfiguration globalConfiguration;

   @Inject
   BlockingManager blockingManager;

   private final Map<String, CompletionStage<RemoteCacheManager>> remoteContainers = new HashMap<>();

   @Start
   public void start() {
      RemoteContainersConfiguration configuration = globalConfiguration.module(RemoteContainersConfiguration.class);
      if (configuration != null) {
         for (Map.Entry<String, RemoteContainerConfiguration> e : configuration.configurations().entrySet()) {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.uri(e.getValue().uri()).transportFactory(configuration.transportFactory());
            remoteContainers.put(e.getKey(), blockingManager.supplyBlocking(() -> new RemoteCacheManager(builder.build()), "RemoteContainer-create"));
         }
      }
   }

   @Stop
   public void stop() {
      remoteContainers.values().forEach(c -> c.thenAccept(RemoteCacheManager::stop));
   }

   @Override
   public CompletionStage<RemoteCacheManager> cacheContainer(String name) {
      return remoteContainers.get(name);
   }
}
