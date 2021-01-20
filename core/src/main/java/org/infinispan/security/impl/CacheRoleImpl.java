package org.infinispan.security.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
   private final boolean inheritable;

   public CacheRoleImpl(String name, boolean inheritable, AuthorizationPermission... authorizationPermissions) {
      this(name, inheritable, new HashSet<>(Arrays.asList(authorizationPermissions)));
   }

   public CacheRoleImpl(String name, boolean inheritable, Set<AuthorizationPermission> permissions) {
      this.name = name;
      this.permissions = Collections.unmodifiableSet(permissions);
      int permMask = 0;
      for (AuthorizationPermission permission : permissions) {
         permMask |= permission.getMask();
      }
      this.mask = permMask;
      this.inheritable = inheritable;
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
   public boolean isInheritable() {
      return inheritable;
   }

   @Override
   public String toString() {
      return "CacheRoleImpl{" +
            "name='" + name + '\'' +
            ", permissions=" + permissions +
            ", mask=" + mask +
            ", inheritable=" + inheritable +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheRoleImpl cacheRole = (CacheRoleImpl) o;

      if (mask != cacheRole.mask) return false;
      if (!name.equals(cacheRole.name)) return false;
      return permissions.equals(cacheRole.permissions);
   }

   @Override
   public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + permissions.hashCode();
      result = 31 * result + mask;
      return result;
   }
}
