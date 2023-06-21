package org.infinispan.security;

import java.util.Map;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Maps roles to permissions
 *
 * @since 14.0
 **/
@Scope(Scopes.GLOBAL)
public interface RolePermissionMapper {
   /**
    * Sets the context for this {@link RolePermissionMapper}
    *
    * @param context
    */
   default void setContext(AuthorizationMapperContext context) {}

   /**
    * @param name the name of the role
    * @return the {@link Role}
    */
   Role getRole(String name);

   /**
    * @return all roles handled by this RolePermissionMapper
    */
   Map<String, Role> getAllRoles();

   /**
    * @param name
    * @return whether this permission mapper contains the named role
    */
   boolean hasRole(String name);
}
