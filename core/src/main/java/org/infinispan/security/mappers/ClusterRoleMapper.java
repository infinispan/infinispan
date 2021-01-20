package org.infinispan.security.mappers;

import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.security.MutablePrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * ClusterRoleMapper.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ClusterRoleMapper implements MutablePrincipalRoleMapper {
   private EmbeddedCacheManager cacheManager;
   private static final String CLUSTER_ROLE_MAPPER_CACHE = "org.infinispan.ROLES";
   private Cache<String, RoleSet> clusterRoleMap;

   private Cache<String, RoleSet> getClusterRoleMap() {
      if (clusterRoleMap == null) {
         if (cacheManager != null) {
            clusterRoleMap = cacheManager.getCache(CLUSTER_ROLE_MAPPER_CACHE);
         }
      }
      return clusterRoleMap;
   }

   @Override
   public Set<String> principalToRoles(Principal principal) {
      Cache<String, RoleSet> roleMap = getClusterRoleMap();
      if (roleMap == null) {
         return Collections.singleton(principal.getName());
      }
      RoleSet roleSet = roleMap.get(principal.getName());
      if (!roleSet.roles.isEmpty()) {
         return roleSet.roles;
      } else {
         return Collections.singleton(principal.getName());
      }
   }

   @Override
   public void setContext(PrincipalRoleMapperContext context) {
      this.cacheManager = context.getCacheManager();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false)
            .security().authorization().disable();

      InternalCacheRegistry internalCacheRegistry =
         SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(CLUSTER_ROLE_MAPPER_CACHE, cfg.build(), EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
   }

   @Override
   public void grant(String roleName, String principalName) {
      RoleSet roleSet = getClusterRoleMap().computeIfAbsent(principalName, n -> new RoleSet() );
      roleSet.roles.add(roleName);
      clusterRoleMap.put(principalName, roleSet);
   }

   @Override
   public void deny(String roleName, String principalName) {
      RoleSet roleSet = getClusterRoleMap().computeIfAbsent(principalName, n -> new RoleSet() );
      roleSet.roles.remove(roleName);
      clusterRoleMap.put(principalName, roleSet);
   }

   @Override
   public Set<String> list(String principalName) {
      RoleSet roleSet = getClusterRoleMap().get(principalName);
      if (roleSet != null) {
         return Collections.unmodifiableSet(roleSet.roles);
      } else {
         return Collections.singleton(principalName);
      }
   }

   @Override
   public String listAll() {
      StringBuilder sb = new StringBuilder();
      for(RoleSet set : getClusterRoleMap().values()) {
         sb.append(set.roles.toString());
      }
      return sb.toString();
   }

   @ProtoTypeId(ProtoStreamTypeIds.ROLE_SET)
   public static class RoleSet {
      @ProtoField(number = 1, collectionImplementation = HashSet.class)
      final Set<String> roles;

      RoleSet() {
         this(new HashSet());
      }

      @ProtoFactory
      RoleSet(Set<String> roles) {
         this.roles = roles;
      }


      public Set<String> getRoles() {
         return roles;
      }
   }
}
