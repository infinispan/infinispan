package org.infinispan.security.impl;

import java.security.AccessController;
import java.security.Principal;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.registry.ClusterRegistry;
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
   private ClusterRegistry<String, Subject, Integer> subjectRoleMaskCache;
   private String authCacheScope;

   public AuthorizationManagerImpl() {
   }

   @Inject
   public void init(Cache<?, ?> cache, GlobalConfiguration globalConfiguration, Configuration configuration,
         ClusterRegistry<String, Subject, Integer> clusterRegistry) {
      this.globalConfiguration = globalConfiguration.security();
      this.configuration = configuration.security().authorization();
      this.subjectRoleMaskCache = clusterRegistry;
      authCacheScope = String.format("%s_%s", AuthorizationManager.class.getName(), cache.getName());
   }

   @Override
   public void checkPermission(AuthorizationPermission perm) {
      Subject subject = Subject.getSubject(AccessController.getContext());
      Integer subjectMask = (subject == null) ? Integer.valueOf(0) : null; //ISPN-4056 subjectRoleMaskCache.get(authCacheScope, subject);
      if (subjectMask == null) {
         subjectMask = AuthorizationHelper.computeSubjectRoleMask(subject, globalConfiguration, configuration);
         //ISPN-4056 subjectRoleMaskCache.put(authCacheScope, subject, subjectMask, globalConfiguration.securityCacheTimeout(), TimeUnit.MILLISECONDS);
      }
      AuthorizationHelper.checkPermission(subject, subjectMask, perm);
   }
}
