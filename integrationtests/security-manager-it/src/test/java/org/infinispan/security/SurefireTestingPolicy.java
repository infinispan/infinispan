package org.infinispan.security;

import java.io.File;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.HashSet;
import java.util.Set;

public class SurefireTestingPolicy extends Policy {
   static final String MODULE_CLASSES = "/classes/";
   static final String MODULE_TEST_CLASSES = "/test-classes/";
   static final Set<String> grants = new HashSet<String>();
   boolean logPolicyChecks = false; // switch to true if we need to log grants

   @Override
   public boolean implies(ProtectionDomain domain, Permission permission) {
      String location = domain.getCodeSource().getLocation().getPath().replaceAll("\\\\", "/");
      // Allow any permissions from dependencies and the actual modules' classes
      if (location.endsWith(".jar") || location.endsWith(MODULE_CLASSES)) {
         if (logPolicyChecks) {
            StringBuilder sb = new StringBuilder();
            sb.append(location.substring(location.lastIndexOf(File.separator)+1));
            sb.append("> permission ");
            sb.append(permission.getClass().getName());
            sb.append(" \"");
            sb.append(permission.getName());
            sb.append("\"");
            if (permission.getActions().length()>0) {
               sb.append(" \"");
               sb.append(permission.getActions());
               sb.append("\"");
            }
            sb.append(";");
            String grant = sb.toString();
            if(!grants.contains(grant)) {
               grants.add(grant);
               System.out.println(grant);
            }
         }
         return true;
      }
      // For simplicity deny just our own SecurityPermissions.
      if (location.endsWith(MODULE_TEST_CLASSES) && !(permission instanceof CachePermission)) {
         return true;
      }
      // Separate from the above condition to allow setting breakpoints
      return false;
   }

}
