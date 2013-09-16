package org.infinispan.lifecycle;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * ModuleLifecycle is an internal API hook for delegating lifecycle events to Infinispan sub-modules.
 * <p>
 * For example, the 'tree' module needs to register specific types with the StreamingMarshaller. The 'query'
 * module needs to register an interceptor with the Cache if the Cache has enabled querying etc etc.
 * <p />
 * To use this hook, you would need to implement this interface (or extend {@link AbstractModuleLifecycle})
 * and then create a file called <tt>infinispan-module.properties</tt> in the root of your module's JAR.
 * When using Maven, for example, <tt>infinispan-module.properties</tt> would typically be in the module's
 * <tt>src/main/resources</tt> directory so it gets packaged appropriately.
 * <p />
 * <u>infinispan-module.properties</u>
 * <p />
 * Currently, the following properties are supported:
 * <ul>
 * <li><tt>infinispan.module.name</tt> - the name of the module.  The aim of this property is to identify
 * each individual module, so the contents can be any valid String as long as it does not clash with the
 * module name of another module's life cycle properties. </li>
 * <li><tt>infinispan.module.lifecycle</tt> - the name of the class implementing {@link ModuleLifecycle}.
 * This implementation would typically reside in the module's codebase.</li>
 * </ul>
 * Modules who also have their own configuration (see {@see org.infinispan.configuration}), can access their
 * configuration beans via {@link Configuration#module(Class)}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ModuleLifecycle {

    void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration);

    void cacheManagerStarted(GlobalComponentRegistry gcr);

    void cacheManagerStopping(GlobalComponentRegistry gcr);

    void cacheManagerStopped(GlobalComponentRegistry gcr);

    void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName);

    void cacheStarted(ComponentRegistry cr, String cacheName);

    void cacheStopping(ComponentRegistry cr, String cacheName);

    void cacheStopped(ComponentRegistry cr, String cacheName);

}
