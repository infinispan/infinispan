package org.infinispan.security.impl;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.GlobalSecurityManager;

/**
 * AuthorizationManagerImpl. An implementation of the {@link AuthorizationManager} interface.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationManagerImpl implements AuthorizationManager {
   private AuthorizationConfiguration configuration;
   private AuthorizationHelper authzHelper;

   public AuthorizationManagerImpl() {
   }

   @Inject
   public void init(@ComponentName(KnownComponentNames.CACHE_NAME) String cacheName,
                    GlobalConfiguration globalConfiguration, Configuration configuration,
                    GlobalSecurityManager globalSecurityManager, EmbeddedCacheManager cacheManager) {
      this.configuration = configuration.security().authorization();
      Cache<CachePrincipalPair, SubjectACL> globalACLCache =
            (Cache<CachePrincipalPair, SubjectACL>) globalSecurityManager.globalACLCache();
      this.authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHE, cacheName,
            globalACLCache);
      if (globalACLCache != null) {
         cacheManager.addCacheDependency(cacheName, globalACLCache.getName());
      }
   }

   @Override
   public void checkPermission(AuthorizationPermission perm) {
      authzHelper.checkPermission(configuration, null, perm, null);
   }

   @Override
   public void checkPermission(Subject subject, AuthorizationPermission perm) {
      authzHelper.checkPermission(configuration, subject, perm, null);
   }

   @Override
   public void checkPermission(AuthorizationPermission perm, String role) {
      authzHelper.checkPermission(configuration, null, perm, role);
   }

   @Override
   public void checkPermission(Subject subject, AuthorizationPermission perm, String role) {
      authzHelper.checkPermission(configuration, subject, perm, role);
   }
}
