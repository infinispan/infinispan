package org.infinispan.test.integration.security.utils;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.PrincipalRoleMapperContext;
import org.jboss.security.SimpleGroup;
import org.jboss.security.SimplePrincipal;

/**
 * 
 * SimplePrincipalGroupRoleMapper maps names of principals contained in {@link SimpleGroup} to
 * roles. These principals are assumed to be {@link SimplePrincipal}s. {@link SimpleGroup} of
 * {@link SimplePrincipal}s is returned by WildFly logging modules, e.g. AdvancedLdapLoginModule.
 * 
 * @author vjuranek
 * @since 7.0
 */
public class SimplePrincipalGroupRoleMapper implements PrincipalRoleMapper {

   @Override
   public Set<String> principalToRoles(Principal principal) {
      if (principal instanceof SimpleGroup) {
         Enumeration<Principal> members = ((SimpleGroup) principal).members();
         if (members.hasMoreElements()) {
            Set<String> roles = new HashSet<String>();
            while (members.hasMoreElements()) {
               Principal innerPrincipal = members.nextElement();
               if (innerPrincipal instanceof SimplePrincipal) {
                  SimplePrincipal sp = (SimplePrincipal) innerPrincipal;
                  roles.add(sp.getName());
               }
            }
            return roles;
         } 
      }
      return null;
   }
   
   @Override
   public void setContext(PrincipalRoleMapperContext context) {
      // Do nothing
   }
}
