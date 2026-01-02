package org.infinispan.security;

import java.util.Map;
import java.util.Set;

import org.infinispan.security.mappers.ClusterRoleMapper;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface MutablePrincipalRoleMapper extends PrincipalRoleMapper {
   void grant(String roleName, String principalName);

   void deny(String roleName, String principalName);

   Set<String> list(String principalName);

   String listAll();

   Set<String> listPrincipals();

   Set<Map.Entry<String, ClusterRoleMapper.RoleSet>> listPrincipalsAndRoleSet();

   Set<String> listPrincipalsByRole(String role);

   void denyAll(String principal);
}
