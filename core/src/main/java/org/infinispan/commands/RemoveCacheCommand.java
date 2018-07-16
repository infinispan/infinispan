package org.infinispan.commands;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Command to stop a cache and remove all its contents from both
 * memory and any backing store.
 *
 * @author Galder Zamarreño
 * @since 5.0
 * @deprecated Use {@link org.infinispan.commons.api.CacheContainerAdmin#removeCache(String)} instead
 */
@Deprecated // TODO remove in 10.0
public class RemoveCacheCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 18;

   private EmbeddedCacheManager cacheManager;

   private RemoveCacheCommand() {
      super(null); // For command id uniqueness test
   }

   public RemoveCacheCommand(ByteString cacheName, EmbeddedCacheManager cacheManager) {
      super(cacheName);
      this.cacheManager = cacheManager;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      removeCache(cacheManager, cacheName.toString());
      return CompletableFutures.completedNull();
   }

   public static void removeCache(EmbeddedCacheManager cacheManager, String cacheName) {
      GlobalComponentRegistry globalComponentRegistry = cacheManager.getGlobalComponentRegistry();
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      if (cacheComponentRegistry != null) {
         cacheComponentRegistry.getComponent(PersistenceManager.class).setClearOnStop(true);
         cacheComponentRegistry.getComponent(CacheJmxRegistration.class).setUnregisterCacheMBean(true);
         cacheComponentRegistry.getComponent(PassivationManager.class).skipPassivationOnStop(true);
         Cache<?, ?> cache = cacheManager.getCache(cacheName, false);
         if (cache != null) {
            cache.stop();
         }
      }
      globalComponentRegistry.removeCache(cacheName);
      // Remove cache configuration and remove it from the computed cache name list
      globalComponentRegistry.getComponent(ConfigurationManager.class).removeConfiguration(cacheName);
      // Remove cache from dependency graph
      //noinspection unchecked
      globalComponentRegistry.getComponent(DependencyGraph.class, CACHE_DEPENDENCY_GRAPH).remove(cacheName);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      // No parameters
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // No parameters
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
   @Override
   public String toString() {
      return "RemoveCacheCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }

}
