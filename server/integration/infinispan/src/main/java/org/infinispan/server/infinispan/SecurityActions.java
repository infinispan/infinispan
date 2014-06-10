package org.infinispan.server.infinispan;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.security.actions.GetCacheComponentRegistryAction;
import org.infinispan.security.actions.GetCacheInterceptorChainAction;
import org.infinispan.security.actions.GetCacheLockManagerAction;
import org.infinispan.security.actions.GetCacheManagerAddress;
import org.infinispan.security.actions.GetCacheManagerClusterNameAction;
import org.infinispan.security.actions.GetCacheManagerCoordinatorAddress;
import org.infinispan.security.actions.GetCacheManagerIsCoordinatorAction;
import org.infinispan.security.actions.GetCacheManagerStatusAction;
import org.infinispan.security.actions.GetCacheRpcManagerAction;
import org.infinispan.security.actions.GetCacheStatusAction;
import org.infinispan.util.concurrent.locks.LockManager;
import org.jboss.as.clustering.infinispan.DefaultEmbeddedCacheManager;

/**
 * SecurityActions for the org.infinispan.server.infinispan package
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SecurityActions {
    private static <T> T doPrivileged(PrivilegedAction<T> action) {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return Security.doPrivileged(action);
        }
    }

    private static <T> T doPrivileged(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        if (System.getSecurityManager() != null) {
            return AccessController.doPrivileged(action);
        } else {
            return Security.doPrivileged(action);
        }
    }

    public static void registerAndStartContainer(final EmbeddedCacheManager container, final Object listener) {
        PrivilegedAction<Void> action = new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                container.addListener(listener);
                container.start();
                return null;
            }
        };
        doPrivileged(action);
    }

    public static boolean stopAndUnregisterContainer(final EmbeddedCacheManager container, final Object listener) {
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
        return doPrivileged(action);
    }

    public static void defineContainerConfiguration(final EmbeddedCacheManager container, final String name,
            final Configuration config) {
        PrivilegedAction<Void> action = new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                container.defineConfiguration(name, config);
                return null;
            }
        };
        doPrivileged(action);
    }

    public static <K, V> Cache<K, V> startCache(final EmbeddedCacheManager container, final String name) {
        PrivilegedAction<Cache<K, V>> action = new PrivilegedAction<Cache<K, V>>() {
            @Override
            public Cache<K, V> run() {
                Cache<K, V> cache = container.getCache(name);
                cache.start();
                return cache;
            }
        };
        return doPrivileged(action);
    }

    public static LockManager getLockManager(final AdvancedCache<?, ?> cache) {
        GetCacheLockManagerAction action = new GetCacheLockManagerAction(cache);
        return doPrivileged(action);
    }

    public static List<CommandInterceptor> getInterceptorChain(final AdvancedCache<?, ?> cache) {
        GetCacheInterceptorChainAction action = new GetCacheInterceptorChainAction(cache);
        return doPrivileged(action);
    }

    public static RpcManager getRpcManager(final AdvancedCache<?, ?> cache) {
        GetCacheRpcManagerAction action = new GetCacheRpcManagerAction(cache);
        return doPrivileged(action);
    }

    public static ComponentRegistry getComponentRegistry(final AdvancedCache<?, ?> cache) {
        GetCacheComponentRegistryAction action = new GetCacheComponentRegistryAction(cache);
        return doPrivileged(action);
    }

    public static ComponentStatus getCacheStatus(AdvancedCache<?, ?> cache) {
        GetCacheStatusAction action = new GetCacheStatusAction(cache);
        return doPrivileged(action);
    }

    public static ComponentStatus getCacheManagerStatus(EmbeddedCacheManager cacheManager) {
        GetCacheManagerStatusAction action = new GetCacheManagerStatusAction(cacheManager);
        return doPrivileged(action);
    }

    public static Address getCacheManagerLocalAddress(DefaultEmbeddedCacheManager cacheManager) {
        GetCacheManagerAddress action = new GetCacheManagerAddress(cacheManager);
        return doPrivileged(action);
    }

    public static Address getCacheManagerCoordinatorAddress(DefaultEmbeddedCacheManager cacheManager) {
        GetCacheManagerCoordinatorAddress action = new GetCacheManagerCoordinatorAddress(cacheManager);
        return doPrivileged(action);
    }

    public static boolean getCacheManagerIsCoordinator(DefaultEmbeddedCacheManager cacheManager) {
        GetCacheManagerIsCoordinatorAction action = new GetCacheManagerIsCoordinatorAction(cacheManager);
        return doPrivileged(action);
    }

    public static String getCacheManagerClusterName(DefaultEmbeddedCacheManager cacheManager) {
        GetCacheManagerClusterNameAction action = new GetCacheManagerClusterNameAction(cacheManager);
        return doPrivileged(action);
    }

    public static Map<String, String> executeInterpreter(final Interpreter interpreter, final String sessionId,
            final String command) throws Exception {
        PrivilegedExceptionAction<Map<String, String>> action = new PrivilegedExceptionAction<Map<String, String>>() {
            @Override
            public Map<String, String> run() throws Exception {
                return interpreter.execute(sessionId, command);
            }
        };
        return doPrivileged(action);
    }


}
