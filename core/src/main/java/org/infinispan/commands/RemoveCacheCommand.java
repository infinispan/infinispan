package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.manager.EmbeddedCacheManager;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

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
   private GlobalComponentRegistry registry;
   private PersistenceManager persistenceManager;
   private CacheJmxRegistration cacheJmxRegistration;

   private RemoveCacheCommand() {
      super(null); // For command id uniqueness test
   }

   public RemoveCacheCommand(String cacheName, EmbeddedCacheManager cacheManager,
         GlobalComponentRegistry registry, PersistenceManager persistenceManager,
         CacheJmxRegistration cacheJmxRegistration) {
      super(cacheName);
      this.cacheManager = cacheManager;
      this.registry = registry;
      this.persistenceManager = persistenceManager;
      this.cacheJmxRegistration = cacheJmxRegistration;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      persistenceManager.setClearOnStop(true);
      cacheJmxRegistration.setUnregisterCacheMBean(true);
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      cache.stop();
      registry.removeCache(cacheName);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
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
