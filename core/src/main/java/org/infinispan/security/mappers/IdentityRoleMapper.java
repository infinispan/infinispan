package org.infinispan.security.mappers;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.security.PrincipalRoleMapper;

/**
 * IdentityRoleMapper. A very simple role which simply returns the principal's name as the role name.
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @api.public
 */
@Scope(Scopes.GLOBAL)
public class IdentityRoleMapper implements PrincipalRoleMapper {

   @Override
   public Set<String> principalToRoles(Principal principal) {
      return Collections.singleton(principal.getName());
   }

   @Override
   public boolean equals(Object obj) {
      return obj instanceof IdentityRoleMapper;
   }
}
