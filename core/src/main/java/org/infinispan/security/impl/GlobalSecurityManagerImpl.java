package org.infinispan.security.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.GlobalSecurityManager;

/**
 * GlobalSecurityManagerImpl. Initialize the global ACL cache.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MBean(objectName = "GlobalSecurityManager", description = "Controls global ACL caches")
public class GlobalSecurityManagerImpl implements GlobalSecurityManager {
   private static final String ACL_CACHE = "___acl_cache";
   private EmbeddedCacheManager cacheManager;
   private boolean cacheEnabled;

   @Inject
   public void init(EmbeddedCacheManager cacheManager, GlobalConfiguration globalConfiguration, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      registerGlobalACLCacheConfiguration(globalConfiguration, internalCacheRegistry);
   }

   private void registerGlobalACLCacheConfiguration(GlobalConfiguration globalConfiguration, InternalCacheRegistry internalCacheRegistry) {
      long timeout = globalConfiguration.security().securityCacheTimeout();
      if (timeout != 0) {
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cfg.simpleCache(true);
         if (timeout > 0)
            cfg.expiration().lifespan(timeout);
         internalCacheRegistry.registerInternalCache(ACL_CACHE, cfg.build());
         cacheEnabled = true;
      } else {
         cacheEnabled = false;
      }
   }

   @Override
   public Cache<?, ?> globalACLCache() {
      if (cacheEnabled) {
         return cacheManager.getCache(ACL_CACHE);
      } else {
         return null;
      }
   }

   @ManagedOperation(name="Flush ACL Cache", displayName="Flush ACL Cache", description="Flushes the global ACL cache for this node")
   @Override
   public void flushGlobalACLCache() {
      if (cacheEnabled) {
         globalACLCache().clear();
      }
   }
}
