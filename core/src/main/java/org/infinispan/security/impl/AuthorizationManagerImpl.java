package org.infinispan.security.impl;

import java.security.Principal;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * AuthorizationManagerImpl. An implementation of the {@link AuthorizationManager} interface. In
 * order to increase performance, the access mask computed from the {@link Subject}'s
 * {@link Principal}s is cached for future uses.
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
   public void init(Cache<?, ?> cache, GlobalConfiguration globalConfiguration, Configuration configuration,
         ClusterRegistry<String, Subject, Integer> clusterRegistry) {
      this.globalConfiguration = globalConfiguration.security();
      this.configuration = configuration.security().authorization();
      this.authzHelper = new AuthorizationHelper(this.globalConfiguration, AuditContext.CACHE, cache.getName(), clusterRegistry);
   }

   @Override
   public void checkPermission(AuthorizationPermission perm) {
      authzHelper.checkPermission(configuration, perm);
   }
}
