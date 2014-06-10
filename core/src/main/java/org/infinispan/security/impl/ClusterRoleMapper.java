package org.infinispan.security.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * ClusterPrincipalMapper.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ClusterRoleMapper implements PrincipalRoleMapper {
   private EmbeddedCacheManager cacheManager;
   private ClusterRegistry<Class<?>, String, Set<String>> clusterRegistry;

   @Override
   public Set<String> principalToRoles(Principal principal) {
      if (clusterRegistry != null) {
         return clusterRegistry.get(ClusterRoleMapper.class, principal.getName());
      } else {
         return Collections.singleton(principal.getName());
      }
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setContext(PrincipalRoleMapperContext context) {
      this.cacheManager = context.getCacheManager();
      clusterRegistry = cacheManager.getGlobalComponentRegistry().getComponent(ClusterRegistry.class);
   }

   public void grant(String roleName, String principalName) {
      Set<String> roleSet = clusterRegistry.get(ClusterRoleMapper.class, principalName);
      if (roleSet == null) {
         roleSet = new HashSet<String>();
      }
      roleSet.add(roleName);
      clusterRegistry.put(ClusterRoleMapper.class, principalName, roleSet);
   }

   public void deny(String roleName, String principalName) {
      Set<String> roleSet = clusterRegistry.get(ClusterRoleMapper.class, principalName);
      if (roleSet == null) {
         roleSet = new HashSet<String>();
      }
      roleSet.remove(roleName);
      clusterRegistry.put(ClusterRoleMapper.class, principalName, roleSet);
   }

   public Set<String> list(String principalName) {
      Set<String> roleSet = clusterRegistry.get(ClusterRoleMapper.class, principalName);
      if (roleSet != null) {
         return Collections.unmodifiableSet(roleSet);
      } else {
         return InfinispanCollections.emptySet();
      }
   }

   public String listAll() {
      Set<String> principals = clusterRegistry.keys(ClusterRoleMapper.class);
      StringBuilder sb = new StringBuilder();
      for(String principal : principals) {
         sb.append(list(principal));
      }
      return sb.toString();
   }
}
