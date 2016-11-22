package org.infinispan.jmx;

import org.infinispan.security.Security;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

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
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return Security.doPrivileged(action);
        }
    }

    static void registerMBean(Object mbean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
        doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                mBeanServer.registerMBean(mbean, objectName);
            } catch (Exception e) {
            }
            return null;
        });
    }

    static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
        doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                mBeanServer.unregisterMBean(objectName);
            } catch (Exception e) {
            }
            return null;
        });
    }

    static Set<ObjectName> queryNames(ObjectName target, QueryExp query, MBeanServer mBeanServer) throws MalformedObjectNameException {
        final Set<ObjectName> results = new HashSet<>();

        return doPrivileged(() -> {
            results.addAll(mBeanServer.queryNames(target, query));
            return results;
        });

    }
}
