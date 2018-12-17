package org.infinispan.security.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * ClusterRoleMapper.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ClusterRoleMapper implements PrincipalRoleMapper {
   private EmbeddedCacheManager cacheManager;
   private static final String CLUSTER_ROLE_MAPPER_CACHE = "___cluster_role_mapper";
   private Cache<String, Set<String>> clusterRoleMap;

   private Cache<String, Set<String>> getClusterRoleMap() {
      if (clusterRoleMap == null) {
         if (cacheManager != null) {
            clusterRoleMap = cacheManager.getCache(CLUSTER_ROLE_MAPPER_CACHE);
         }
      }
      return clusterRoleMap;
   }

   @Override
   public Set<String> principalToRoles(Principal principal) {
      if (getClusterRoleMap() != null) {
         return clusterRoleMap.get(principal.getName());
      } else {
         return Collections.singleton(principal.getName());
      }
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setContext(PrincipalRoleMapperContext context) {
      this.cacheManager = context.getCacheManager();
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false)
            .security().authorization().disable();

      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(CLUSTER_ROLE_MAPPER_CACHE, cfg.build(), EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
   }

   public void grant(String roleName, String principalName) {
      Set<String> roleSet = getClusterRoleMap().computeIfAbsent(principalName, n -> new HashSet<>() );
      roleSet.add(roleName);
      clusterRoleMap.put(principalName, roleSet);
   }

   public void deny(String roleName, String principalName) {
      Set<String> roleSet = getClusterRoleMap().computeIfAbsent(principalName, n -> new HashSet<>() );
      roleSet.remove(roleName);
      clusterRoleMap.put(principalName, roleSet);
   }

   public Set<String> list(String principalName) {
      Set<String> roleSet = getClusterRoleMap().get(principalName);
      if (roleSet != null) {
         return Collections.unmodifiableSet(roleSet);
      } else {
         return Collections.emptySet();
      }
   }

   public String listAll() {
      StringBuilder sb = new StringBuilder();
      for(Set<String> set : getClusterRoleMap().values()) {
         sb.append(set.toString());
      }
      return sb.toString();
   }
}
