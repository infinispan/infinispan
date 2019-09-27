package org.infinispan.lifecycle;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * ModuleLifecycle is an API hook for delegating lifecycle events to Infinispan sub-modules.
 * <p>
 * For example the 'query' module needs to register an interceptor with the Cache if the Cache has querying enabled etc.
 * <p>
 * To use this hook, you would need to implement this interface and annotate it with
 * {@link org.infinispan.factories.annotations.InfinispanModule}.
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
