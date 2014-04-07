package org.infinispan.security;

import java.io.File;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.Provider;

public class SurefireTestingPolicy extends Policy {
   static final String MODULE_CLASSES = File.separator + "classes" + File.separator;
   static final String MODULE_TEST_CLASSES = File.separator + "test-classes" + File.separator;

   @Override
   public Provider getProvider() {
      return super.getProvider();
   }

   @Override
   public String getType() {
      return super.getType();
   }

   @Override
   public Parameters getParameters() {
      return super.getParameters();
   }

   @Override
   public PermissionCollection getPermissions(CodeSource codesource) {
      return super.getPermissions(codesource);
   }

   @Override
   public PermissionCollection getPermissions(ProtectionDomain domain) {
      return super.getPermissions(domain);
   }

   @Override
   public void refresh() {
      super.refresh();
   }

   @Override
   public boolean implies(ProtectionDomain domain, Permission permission) {
      String location = domain.getCodeSource().getLocation().getPath();
      // Allow any permissions from dependencies and the actual modules' classes
      if (location.endsWith(".jar") || location.endsWith(MODULE_CLASSES)) {
         return true;
      }
      // For simplicity deny just our own SecurityPermissions.
      if (location.endsWith(MODULE_TEST_CLASSES) && !(permission instanceof SecurityPermission)) {
         return true;
      }
      // Separate from the above condition to allow setting breakpoints
      return false;
   }

}
