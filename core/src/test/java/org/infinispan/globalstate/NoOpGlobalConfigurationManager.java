package org.infinispan.globalstate;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.TestingUtil;

/*
 * A no-op implementation for tests which mess up with initial state transfer and RPCs
 */
public class NoOpGlobalConfigurationManager implements GlobalConfigurationManager {
   @Override
   public Cache<ScopedState, Object> getStateCache() {
      return null;
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   public static void amendCacheManager(EmbeddedCacheManager cm) {
      TestingUtil.replaceComponent(cm, PrincipalRoleMapper.class, new IdentityRoleMapper(), true);
      TestingUtil.replaceComponent(cm, RolePermissionMapper.class, new RolePermissionMapper() {
         @Override
         public Role getRole(String name) {
            return null;
         }

         @Override
         public Map<String, Role> getAllRoles() {
            return Collections.emptyMap();
         }

         @Override
         public boolean hasRole(String name) {
            return false;
         }
      }, true);
      TestingUtil.replaceComponent(cm, GlobalConfigurationManager.class, new NoOpGlobalConfigurationManager(), true);
   }
}
