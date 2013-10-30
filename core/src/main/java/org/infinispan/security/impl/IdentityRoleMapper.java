package org.infinispan.security.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.infinispan.security.PrincipalRoleMapper;

/**
 * IdentityRoleMapper. A very simple role which simply returns the principal's name as the role name.
 * This is probably only useful for tests.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class IdentityRoleMapper implements PrincipalRoleMapper {

   @Override
   public Set<String> principalToRoles(Principal principal) {
      return Collections.singleton(principal.getName());
   }

}
