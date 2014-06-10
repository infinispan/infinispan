package org.infinispan.security.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * CommonNameRoleMapper. A simple mapper which extracts the Common Name (CN) from an
 * LDAP-style Distinguished Name (DN) and returns it as the role.
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @public
 */
public class CommonNameRoleMapper implements PrincipalRoleMapper {

   @Override
   public Set<String> principalToRoles(Principal principal) {
      String name = principal.getName();
      if (name.startsWith("CN=")) {
         return Collections.singleton(name.substring(3, name.indexOf(',')));
      } else {
         return null;
      }
   }

   @Override
   public void setContext(PrincipalRoleMapperContext context) {
      // Do nothing
   }

}
