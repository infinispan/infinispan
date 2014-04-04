package org.infinispan.security;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * CachePermissionCollection.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class CachePermissionCollection extends PermissionCollection {
   private static final Log log = LogFactory.getLog(CachePermissionCollection.class);
   private static final long serialVersionUID = -3709477547317792941L;
   private final List<Permission> perms;
   private int mask = 0;

   public CachePermissionCollection() {
      perms = new ArrayList<Permission>();
   }

   @Override
   public void add(Permission permission) {
      if (permission.getClass() != CachePermission.class)
         throw log.invalidPermission(permission);

     if (isReadOnly())
         throw log.readOnlyPermissionCollection();

     CachePermission p = (CachePermission)permission;

     synchronized (this) {
         perms.add(p);
         mask |= p.getAuthorizationPermission().getMask();
     }
   }

   @Override
   public boolean implies(Permission permission) {
      if (permission == null || !permission.getClass().equals(CachePermission.class))
         return false;
      CachePermission p = (CachePermission)permission;
      return p.getAuthorizationPermission().matches(mask);
   }

   @Override
   public Enumeration<Permission> elements() {
      synchronized (this) {
         return Collections.enumeration(perms);
     }
   }

}
