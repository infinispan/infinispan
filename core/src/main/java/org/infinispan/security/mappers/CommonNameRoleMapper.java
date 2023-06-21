package org.infinispan.security.mappers;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.security.PrincipalRoleMapper;

/**
 * CommonNameRoleMapper. A simple mapper which extracts the Common Name (CN) from an
 * LDAP-style Distinguished Name (DN) and returns it as the role.
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @api.public
 */
@Scope(Scopes.GLOBAL)
public class CommonNameRoleMapper implements PrincipalRoleMapper {

   @Override
   public Set<String> principalToRoles(Principal principal) {
      String name = principal.getName();
      if (name.regionMatches(true, 0, "CN=", 0, 3)) {
         return Collections.singleton(name.split(",")[0].substring(3));
      } else {
         return null;
      }
   }

   @Override
   public boolean equals(Object obj) {
      return obj instanceof CommonNameRoleMapper;
   }
}
