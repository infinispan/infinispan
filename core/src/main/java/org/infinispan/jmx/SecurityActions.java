package org.infinispan.jmx;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.jmx package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ingo Weiss
 * @since 9.0
 */
final class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return (System.getSecurityManager() != null) ? AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static Set<ObjectName> queryNames(ObjectName target, QueryExp query, MBeanServer mBeanServer) {
      return doPrivileged(() -> mBeanServer.queryNames(target, query));
   }
}
