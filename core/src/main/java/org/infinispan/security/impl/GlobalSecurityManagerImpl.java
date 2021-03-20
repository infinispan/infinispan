package org.infinispan.security.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.GlobalSecurityManager;
import org.infinispan.util.concurrent.CompletableFutures;

import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * GlobalSecurityManagerImpl. Initialize the global ACL cache.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MBean(objectName = "GlobalSecurityManager", description = "Controls global ACL caches")
@Scope(Scopes.GLOBAL)
public class GlobalSecurityManagerImpl implements GlobalSecurityManager {
   private boolean cacheEnabled;
   private com.github.benmanes.caffeine.cache.Cache<CachePrincipalPair, SubjectACL> cache;
   private EmbeddedCacheManager embeddedCacheManager;

   @Inject
   public void init(GlobalConfiguration globalConfiguration, EmbeddedCacheManager embeddedCacheManager, Authorizer authorizer) {
      long timeout = globalConfiguration.security().securityCacheTimeout();
      long size = globalConfiguration.security().securityCacheSize();
      if (timeout > 0 && size > 0) {
         cache = Caffeine.newBuilder().maximumSize(size).expireAfterWrite(timeout, TimeUnit.MILLISECONDS).build();
         authorizer.setAclCache(cache.asMap());
         cacheEnabled = true;
      } else {
         cacheEnabled = false;
      }
      this.embeddedCacheManager = embeddedCacheManager;
   }

   @Override
   public Map<?, ?> globalACLCache() {
      if (cacheEnabled) {
         return cache.asMap();
      } else {
         return null;
      }
   }

   @ManagedOperation(name = "Flush ACL Cache", displayName = "Flush ACL Cache", description = "Flushes the global ACL cache across the entire cluster")
   @Override
   public CompletionStage<Void> flushGlobalACLCache() {
      if (cacheEnabled) {
         ClusterExecutor executor = SecurityActions.getClusterExecutor(embeddedCacheManager);
         return executor.submitConsumer(cm -> {
            GlobalSecurityManager globalSecurityManager = SecurityActions.getGlobalComponentRegistry(cm).getComponent(GlobalSecurityManager.class);
            ((GlobalSecurityManagerImpl) globalSecurityManager).flushGlobalACLCache0();
            return null;
         }, (a, v, t) -> {
         });
      } else {
         return CompletableFutures.completedNull();
      }
   }

   public void flushGlobalACLCache0() {
      if (cacheEnabled) {
         globalACLCache().clear();
      }
   }
}
