package org.infinispan.security.impl;

import javax.security.auth.Subject;

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
   private Cache<Subject, SubjectACL> aclCache;

   @Inject
   public void init(EmbeddedCacheManager cacheManager, GlobalConfiguration globalConfiguration, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      internalCacheRegistry.registerInternalCache(ACL_CACHE, getGlobalACLCacheConfiguration(globalConfiguration).build());
   }

   private ConfigurationBuilder getGlobalACLCacheConfiguration(GlobalConfiguration globalConfiguration) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.expiration().lifespan(globalConfiguration.security().securityCacheTimeout());
      return cfg;
   }

   @Override
   public Cache<Subject, SubjectACL> getGlobalACLCache() {
      if (aclCache == null) {
         aclCache = cacheManager.getCache(ACL_CACHE);
      }
      return aclCache;
   }

   @ManagedOperation(name="Flush ACL Cache", displayName="Flush ACL Cache", description="Flushes the global ACL cache for this node")
   @Override
   public void flushACLCache() {
      getGlobalACLCache().clear();
   }
}
