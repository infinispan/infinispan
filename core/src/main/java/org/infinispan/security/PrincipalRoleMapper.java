package org.infinispan.security;

import java.security.Principal;
import java.util.Set;

/**
 * PrincipalRoleMapper.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface PrincipalRoleMapper {
   /**
    * Maps a principal name to a set of role names. The principal name depends on
    * the source of the principal itself. For example, in LDAP a Principal
    * might use the Distinguished Name format (DN). The mapper should
    * return null if it does not recognize the principal.
    *
    * @param principal
    * @return
    */
   Set<String> principalToRoles(Principal principal);
}
