package org.jboss.as.clustering.infinispan.subsystem;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.DomainCombiner;
import java.security.PermissionCollection;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * SecurityActions for the org.jboss.as.clustering.infinispan.subsystem.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
    static final DomainCombiner ADMIN_COMBINER;

    static {
        if (System.getSecurityManager() == null) {
            AllPermission allPermission = new AllPermission();
            PermissionCollection all = allPermission.newPermissionCollection();
            all.add(allPermission);
            final ProtectionDomain[] adminDomains = new ProtectionDomain[] { new ProtectionDomain(SecurityActions.class.getProtectionDomain().getCodeSource(), all) };

            ADMIN_COMBINER = new DomainCombiner() {

                @Override
                public ProtectionDomain[] combine(ProtectionDomain[] currentDomains, ProtectionDomain[] assignedDomains) {
                    return adminDomains;
                }
            };
        } else {
            ADMIN_COMBINER = null;
        }
    }

    static void registerAndStartContainer(final EmbeddedCacheManager container, final Object listener) {
        PrivilegedAction<Void> action = new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                container.addListener(listener);
                container.start();
                return null;
            }
        };
        if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(action);
        } else {
            AccessController.doPrivileged(action, new AccessControlContext(AccessController.getContext(), ADMIN_COMBINER));
        }
    }

    static boolean stopAndUnregisterContainer(final EmbeddedCacheManager container, final Object listener) {
        PrivilegedAction<Boolean> action = new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                if (container.getStatus().allowInvocations()) {
                    container.stop();
                    container.removeListener(listener);
                    return true;
                } else {
                    return false;
                }
            }
        };
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return AccessController.doPrivileged(action, new AccessControlContext(AccessController.getContext(), ADMIN_COMBINER));
        }
    }

    static void defineContainerConfiguration(final EmbeddedCacheManager container, final String name, final Configuration config) {
        PrivilegedAction<Void> action = new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                container.defineConfiguration(name, config);
                return null;
            }
        };
        if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(action);
        } else {
            AccessController.doPrivileged(action, new AccessControlContext(AccessController.getContext(), ADMIN_COMBINER));
        }
    }

    static <K, V> Cache<K, V> startCache(final EmbeddedCacheManager container, final String name) {
        PrivilegedAction<Cache<K, V>> action = new PrivilegedAction<Cache<K, V>>() {
            @Override
            public Cache<K, V> run() {
                Cache<K, V> cache = container.getCache(name);
                cache.start();
                return cache;
            }
        };
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return AccessController.doPrivileged(action, new AccessControlContext(AccessController.getContext(), ADMIN_COMBINER));
        }
    }
}
