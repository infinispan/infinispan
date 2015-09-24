package org.infinispan.security.impl;

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
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
   public void init(Cache<?, ?> cache, GlobalConfiguration globalConfiguration, Configuration configuration, GlobalSecurityManager globalSecurityManager) {
      this.configuration = configuration.security().authorization();
      this.authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHE, cache.getName(),
            (ConcurrentMap<CachePrincipalPair, SubjectACL>) globalSecurityManager.globalACLCache());
   }

   @Override
   public void checkPermission(AuthorizationPermission perm) {
      authzHelper.checkPermission(configuration, perm, null);
   }

   @Override
   public void checkPermission(AuthorizationPermission perm, String role) {
      authzHelper.checkPermission(configuration, perm, role);
   }
}
