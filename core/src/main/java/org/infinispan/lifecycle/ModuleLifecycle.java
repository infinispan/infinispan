package org.infinispan.lifecycle;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * ModuleLifecycle is an internal API hook for delegating lifecycle events to Infinispan sub-modules.
 * <p>
 * For example, the 'tree' module needs to register specific types with the StreamingMarshaller. The 'query' module
 * needs to register an interceptor with the Cache if the Cache has enabled querying etc etc.
 * <p>
 * To use this hook, you would need to implement this interface and take the necessary steps to make it discoverable by
 * the {@link java.util.ServiceLoader} mechanism.
 * <p>
 * Modules who also have their own configuration (see {@see org.infinispan.configuration}), can access their
 * configuration beans via {@link Configuration#module(Class)}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ModuleLifecycle {

    default void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {}

    default void cacheManagerStarted(GlobalComponentRegistry gcr) {}

    default void cacheManagerStopping(GlobalComponentRegistry gcr) {}

    default void cacheManagerStopped(GlobalComponentRegistry gcr) {}

    default void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {}

    default void cacheStarted(ComponentRegistry cr, String cacheName) {}

    default void cacheStopping(ComponentRegistry cr, String cacheName) {}

    default void cacheStopped(ComponentRegistry cr, String cacheName) {}
}
