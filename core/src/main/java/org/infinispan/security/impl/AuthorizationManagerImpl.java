package org.infinispan.security.impl;

import java.util.Optional;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
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
   private GlobalSecurityConfiguration globalConfiguration;
   private AuthorizationConfiguration configuration;
   private AuthorizationHelper authzHelper;

   public AuthorizationManagerImpl() {
   }

   @Inject
   public void init(Cache<?, ?> cache, GlobalConfiguration globalConfiguration, Configuration configuration, GlobalSecurityManager globalSecurityManager) {
      this.globalConfiguration = globalConfiguration.security();
      this.configuration = configuration.security().authorization();
      this.authzHelper = new AuthorizationHelper(this.globalConfiguration, AuditContext.CACHE, cache.getName(),
            globalSecurityManager.getGlobalACLCache());
   }

   @Override
   public void checkPermission(AuthorizationPermission perm) {
      authzHelper.checkPermission(configuration, perm, Optional.empty());
   }

   @Override
   public void checkPermission(AuthorizationPermission perm, Optional<String> role) {
      authzHelper.checkPermission(configuration, perm, role);
   }
}
