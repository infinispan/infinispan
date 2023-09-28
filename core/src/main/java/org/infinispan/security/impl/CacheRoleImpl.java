package org.infinispan.security.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Role;

/**
 * CacheRoleImpl.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ROLE)
public class CacheRoleImpl implements Role {
   @ProtoField(number = 1, required = true)
   final String name;
   @ProtoField(number = 2, required = true)
   final boolean inheritable;
   @ProtoField(number = 3, collectionImplementation = HashSet.class)
   final Set<AuthorizationPermission> permissions;
   private final int mask;
   @ProtoField(number = 4)
   final String description;
   @ProtoField(number = 5, required = true)
   final boolean implicit;

   public CacheRoleImpl(String name, String description, boolean isImplicit, boolean inheritable, AuthorizationPermission... authorizationPermissions) {
      this(name, description, isImplicit, inheritable, EnumSet.copyOf(Arrays.asList(authorizationPermissions)));
   }

   @ProtoFactory
   public CacheRoleImpl(String name, String description, boolean implicit, boolean inheritable, Set<AuthorizationPermission> permissions) {
      this.name = name;
      this.description = description;
      this.implicit = implicit;
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
   public String getDescription() {
      return description;
   }

   @Override
   public boolean isImplicit() {
      return implicit;
   }

   @Override
   public String toString() {
      return "CacheRoleImpl{" + "name='" + name + '\'' + ", "
            + "inheritable=" + inheritable + ", permissions=" + permissions
            + ", mask=" + mask + ", description='" + description + '\'' + ", "
            + "implicit=" + implicit + '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      CacheRoleImpl cacheRole = (CacheRoleImpl) o;
      return inheritable == cacheRole.inheritable && mask == cacheRole.mask && implicit == cacheRole.implicit
            && Objects.equals(name, cacheRole.name) && Objects.equals(permissions, cacheRole.permissions)
            && Objects.equals(description, cacheRole.description);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, description, implicit, inheritable, mask);
   }
}
