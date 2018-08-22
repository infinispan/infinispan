package org.infinispan.commons.jmx;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;

/**
 * SecurityActions for the org.infinispan.commons.jmx package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 9.4
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return (System.getSecurityManager() != null) ? AccessController.doPrivileged(action) : action.run();
   }

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

   static Set<ObjectName> queryNames(ObjectName target, QueryExp query, MBeanServer mBeanServer) {
      return doPrivileged(() -> mBeanServer.queryNames(target, query));
   }

   private SecurityActions() {
      // Hide
   }
}
