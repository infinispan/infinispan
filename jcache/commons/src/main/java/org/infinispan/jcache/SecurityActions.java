package org.infinispan.jcache;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

import static java.security.AccessController.doPrivileged;

/**
 * SecurityActions for the org.infinispan.jcache package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ingo Weiss
 * @since 9.0
 */
final class SecurityActions {
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

    static Set<ObjectName> queryNames(ObjectName target, QueryExp query, MBeanServer mBeanServer) {
        final Set<ObjectName> results = new HashSet<>();

        try {
            return doPrivileged((PrivilegedAction<Set<ObjectName>>) () -> {
                results.addAll(mBeanServer.queryNames(target, query));
                return results;
            });
        } catch (Exception e) {
            return results;
        }
    }
}
