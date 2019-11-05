package org.infinispan.jmx;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * SecurityActions for the org.infinispan.jmx package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 9.4
 */
final class SecurityActions {

   private static void doPrivileged(PrivilegedExceptionAction<Void> action) throws Exception {
      try {
         if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(action);
         } else {
            action.run();
         }
      } catch (PrivilegedActionException e) {
         throw e.getException();
      }
   }

   static void registerMBean(Object mbean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      doPrivileged(() -> {
         mBeanServer.registerMBean(mbean, objectName);
         return null;
      });
   }

   static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      doPrivileged(() -> {
         mBeanServer.unregisterMBean(objectName);
         return null;
      });
   }

   private SecurityActions() {
      // Hide
   }
}
