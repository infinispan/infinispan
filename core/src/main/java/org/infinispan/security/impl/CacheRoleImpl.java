package org.infinispan.security.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Role;

/**
 * CacheRoleImpl.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class CacheRoleImpl implements Role {
   private final String name;
   private final Set<AuthorizationPermission> permissions;
   private final int mask;

   public CacheRoleImpl(String name, Set<AuthorizationPermission> permissions) {
      this.name = name;
      this.permissions = Collections.unmodifiableSet(permissions);
      int permMask = 0;
      for (AuthorizationPermission permission : permissions) {
         permMask |= permission.getMask();
      }
      this.mask = permMask;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public Collection<AuthorizationPermission> getPermissions() {
      return permissions;
   }

   @Override
   public int getMask() {
      return mask;
   }

   @Override
   public String toString() {
      return "CacheRoleImpl [name=" + name + ", permissions=" + permissions + ", mask=" + mask + "]";
   }

}
