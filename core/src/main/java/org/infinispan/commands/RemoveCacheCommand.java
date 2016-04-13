package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.DependencyGraph;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;

/**
 * Command to stop a cache and remove all its contents from both
 * memory and any backing store.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
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
   public Object perform(InvocationContext ctx) throws Throwable {
      GlobalComponentRegistry globalComponentRegistry = cacheManager.getGlobalComponentRegistry();
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      String name = cacheName.toString();
      if (cacheComponentRegistry != null) {
         cacheComponentRegistry.getComponent(PersistenceManager.class).setClearOnStop(true);
         cacheComponentRegistry.getComponent(CacheJmxRegistration.class).setUnregisterCacheMBean(true);
         cacheComponentRegistry.getComponent(PassivationManager.class).skipPassivationOnStop(true);
         Cache<?, ?> cache = cacheManager.getCache(name, false);
         if (cache != null) {
            cache.stop();
         }
      }
      globalComponentRegistry.removeCache(name);
      // Remove cache configuration and remove it from the computed cache name list
      globalComponentRegistry.getComponent(ConfigurationManager.class).removeConfiguration(name);
      // Remove cache from dependency graph
      //noinspection unchecked
      globalComponentRegistry.getComponent(DependencyGraph.class, CACHE_DEPENDENCY_GRAPH).remove(cacheName);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
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
