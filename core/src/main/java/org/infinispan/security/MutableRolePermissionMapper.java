package org.infinispan.security;

import java.util.concurrent.CompletionStage;

/**
 * A {@link RolePermissionMapper} with the ability to add/remove roles at runtime
 *
 * @since 14.0
 **/
public interface MutableRolePermissionMapper extends RolePermissionMapper {
   /**
    * Adds a new role
    * @param role the role
    */
   CompletionStage<Void> addRole(Role role);

   /**
    * Removes a role
    * @param role the name of the role to be removed
    * @return true if a role with the supplied name was found and removed
    */
   CompletionStage<Boolean> removeRole(String role);
}
