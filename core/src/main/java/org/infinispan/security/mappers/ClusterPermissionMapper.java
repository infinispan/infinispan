package org.infinispan.security.mappers;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.AuthorizationMapperContext;
import org.infinispan.security.MutableRolePermissionMapper;
import org.infinispan.security.Role;
import org.infinispan.security.RolePermissionMapper;

/**
 * ClusterPermissionMapper. This class implements both a {@link MutableRolePermissionMapper} storing the mappings in a
 * persistent replicated internal cache named <tt>org.infinispan.PERMISSIONS</tt>
 *
 * @author Tristan Tarrant
 * @since 14.0
 */
@Scope(Scopes.GLOBAL)
public class ClusterPermissionMapper implements MutableRolePermissionMapper {
   private static final String CLUSTER_PERMISSION_MAPPER_CACHE = "org.infinispan.PERMISSIONS";
   private EmbeddedCacheManager cacheManager;
   private Cache<String, Role> clusterPermissionMap;
   private Cache<String, Role> clusterPermissionReadMap;

   @Start
   void start() {
      clusterPermissionMap = cacheManager.getCache(CLUSTER_PERMISSION_MAPPER_CACHE);
      clusterPermissionReadMap = clusterPermissionMap.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD, Flag.CACHE_MODE_LOCAL);
   }

   @Override
   public void setContext(AuthorizationMapperContext context) {
      this.cacheManager = context.getCacheManager();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(globalConfiguration.isClustered())
            .security().authorization().disable();
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cacheManager);
      InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(CLUSTER_PERMISSION_MAPPER_CACHE, cfg.build(), EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
      gcr.registerComponent(this, RolePermissionMapper.class);
   }

   @Override
   public CompletionStage<Void> addRole(Role role) {
      return clusterPermissionMap.putAsync(role.getName(), role).thenApply(ignore -> null);
   }

   @Override
   public CompletionStage<Boolean> removeRole(String name) {
      return clusterPermissionMap.removeAsync(name).thenApply(Objects::nonNull);
   }

   @Override
   public Map<String, Role> getAllRoles() {
      return isActive() ? clusterPermissionReadMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) : Collections.emptyMap();
   }

   @Override
   public Role getRole(String name) {
      return isActive() ? clusterPermissionReadMap.get(name) : null;
   }

   @Override
   public boolean hasRole(String name) {
      return isActive() && clusterPermissionReadMap.containsKey(name);
   }

   private boolean isActive() {
      return cacheManager != null && cacheManager.getStatus().allowInvocations();
   }
}
