package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Role;
import org.infinispan.security.impl.CacheRoleImpl;

/**
 * GlobalRoleConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalRoleConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements GlobalRolesConfigurationChildBuilder, Builder<Role> {
   private final Set<AuthorizationPermission> permissions = new HashSet<>();
   private final GlobalAuthorizationConfigurationBuilder builder;
   private final String name;
   private boolean inheritable = true;
   private String description;
   private boolean implicit;

   public GlobalRoleConfigurationBuilder(String name, GlobalAuthorizationConfigurationBuilder builder) {
      super(builder.getGlobalConfig());
      this.builder = builder;
      this.name = name;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Adds a permission to a role
    *
    * @param permission the name of the permission to add to the role. See {@link AuthorizationPermission}
    */
   public GlobalRoleConfigurationBuilder permission(String permission) {
      permissions.add(AuthorizationPermission.valueOf(permission));
      return this;
   }

   /**
    * Adds multiple permissions to a role
    *
    * @param permissions the name of the permissions to add to the role. See {@link AuthorizationPermission}
    */
   public GlobalRoleConfigurationBuilder permission(String... permissions) {
      for (String permission : permissions) {
         this.permissions.add(AuthorizationPermission.valueOf(permission));
      }
      return this;
   }

   /**
    * Adds a permission to a role
    *
    * @param permission the permission to add to the role. See {@link AuthorizationPermission}
    */
   public GlobalRoleConfigurationBuilder permission(AuthorizationPermission permission) {
      permissions.add(permission);
      return this;
   }

   /**
    * Adds multiple permissions to a role
    *
    * @param permissions the permissions to add to the role. See {@link AuthorizationPermission}
    */
   public GlobalRoleConfigurationBuilder permission(AuthorizationPermission... permissions) {
      Collections.addAll(this.permissions, permissions);
      return this;
   }

   @Override
   public GlobalRoleConfigurationBuilder role(String name) {
      return builder.role(name);
   }

   /**
    * Whether this role should be implicitly inherited by secure caches which don't define their roles.
    * @param inheritable
    * @return
    */
   @Override
   public GlobalRoleConfigurationBuilder inheritable(boolean inheritable) {
      this.inheritable = inheritable;
      return this;
   }

   /**
    * A description for the role
    * @param description
    * @return
    */
   @Override
   public GlobalRoleConfigurationBuilder description(String description) {
      this.description = description;
      return this;
   }

   @Override
   public Role create() {
      return new CacheRoleImpl(name, description, implicit, inheritable, permissions);
   }

   @Override
   public Builder<?> read(Role template, Combine combine) {
      permissions.clear();
      permissions.addAll(template.getPermissions());
      inheritable = template.isInheritable();
      description = template.getDescription();
      implicit = template.isImplicit();
      return this;
   }
}
