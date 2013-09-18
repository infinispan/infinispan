package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

import java.util.List;
import java.util.Set;

/**
 * EmbeddedCacheManager is an CacheManager that runs in the same JVM as the client.
 * <p/>
 * Constructing a <tt>EmbeddedCacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.configuration.cache.Configuration} or a path or URL to a configuration XML file: see {@link org.infinispan.manager.DefaultCacheManager}.
 * <p/>
 * Lifecycle - <tt>EmbeddedCacheManager</tt>s have a lifecycle (it implements {@link org.infinispan.lifecycle.Lifecycle}) and
 * the default constructors also call {@link #start()}.  Overloaded versions of the constructors are available, that do
 * not start the <tt>CacheManager</tt>, although it must be kept in mind that <tt>CacheManager</tt>s need to be started
 * before they can be used to create <tt>Cache</tt> instances.
 * <p/>
 * Once constructed, <tt>EmbeddedCacheManager</tt>s should be made available to any component that requires a <tt>Cache</tt>,
 * via <a href="http://en.wikipedia.org/wiki/Java_Naming_and_Directory_Interface">JNDI</a> or via some other mechanism
 * such as an <a href="http://en.wikipedia.org/wiki/Dependency_injection">dependency injection</a> framework.
 * <p/>
 *
 * @see org.infinispan.manager.DefaultCacheManager
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreno
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public interface EmbeddedCacheManager extends CacheContainer, Listenable {

   /**
    * Defines a named cache's configuration using the following algorithm:
    * <p/>
    * Unlike previous versions of Infinispan, this method does not build on an existing configuration (default or named).
    * If you want this behavior, then use {@link ConfigurationBuilder#read(org.infinispan.configuration.cache.Configuration)}.
    * <p/>
    * The other way to define named cache's configuration is declaratively, in the XML file passed in to the cache
    * manager.  This method enables you to override certain properties that have previously been defined via XML.
    * <p/>
    * Passing a brand new Configuration instance as configuration override without having called any of its setters will
    * effectively return the named cache's configuration since no overrides where passed to it.
    *
    * @param cacheName             name of cache whose configuration is being defined
    * @param configurationOverride configuration overrides to use
    * @return a cloned configuration instance
    */
   Configuration defineConfiguration(String cacheName, Configuration configurationOverride);

   /**
    * Defines a named cache's configuration using the following algorithm:
    * <p/>
    * Regardless of whether the cache name has been defined or not, this method creates a clone of the configuration of
    * the cache whose name matches the given template cache name, then applies a clone of the configuration overrides
    * passed in and finally returns this configuration instance.
    * <p/>
    * The other way to define named cache's configuration is declaratively, in the XML file passed in to the cache
    * manager. This method enables you to override certain properties that have previously been defined via XML.
    * <p/>
    * Passing a brand new Configuration instance as configuration override without having called any of its setters will
    * effectively return the named cache's configuration since no overrides where passed to it.
    * <p/>
    * If templateName is null or there isn't any named cache with that name, this methods works exactly like {@link
    * #defineConfiguration(String, Configuration)} in the sense that the base configuration used is the default cache
    * configuration.
    *
    * @param cacheName             name of cache whose configuration is being defined
    * @param templateCacheName     name of cache to which to which apply overrides if cache name has not been previously
    *                              defined
    * @param configurationOverride configuration overrides to use
    * @return a cloned configuration instance
    */
   Configuration defineConfiguration(String cacheName, String templateCacheName, Configuration configurationOverride);

   /**
    * @return the name of the cluster.  Null if running in local mode.
    */
   String getClusterName();

   /**
    * @return the addresses of all the members in the cluster.
    */
   List<Address> getMembers();

   /**
    * @return the address of the local node
    */
   Address getAddress();

   /**
    * @return the address of the cluster's coordinator
    */
   Address getCoordinator();

   /**
    * @return whether the local node is the cluster's coordinator
    */
   boolean isCoordinator();

   /**
    * @return the status of the cache manager
    */
   ComponentStatus getStatus();

   /**
    * Returns global configuration for this CacheManager
    *
    * @return the global configuration object associated to this CacheManager
    */
   GlobalConfiguration getCacheManagerConfiguration();

   /**
    * Returns the configuration for the given cache.
    *
    * @return the configuration for the given cache or null if no such cache is defined
    */
   Configuration getCacheConfiguration(String name);

   /**
    * Returns default configuration for this CacheManager
    *
    * @return the default configuration associated with this CacheManager
    */
   org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration();

   /**
    * This method returns a collection of caches names which contains the
    * caches that have been defined via XML or programmatically, and the
    * caches that have been created at runtime via this cache manager
    * instance.
    *
    * If no named caches are registered or no caches have been created, this
    * method returns an empty set.  The default cache is never included in this
    * set of cache names.
    *
    * @return an immutable set of non-default named caches registered or
    * created with this cache manager.
    */
   Set<String> getCacheNames();

   /**
    * Tests whether a named cache is running.
    * @param cacheName name of cache to test.
    * @return true if the named cache exists and is running; false otherwise.
    */
   boolean isRunning(String cacheName);

   /**
    * Tests whether the default cache is running.
    * @return true if the default cache is running; false otherwise.
    */
   boolean isDefaultRunning();

   /**
    * A cache is considered to exist if it has been created and started via
    * one of the {@link #getCache()} methods and has not yet been removed via
    * {@link #removeCache(String)}. </p>
    *
    * In environments when caches are continuously created and removed, this
    * method offers the possibility to find out whether a cache has either,
    * not been started, or if it was started, whether it's been removed already
    * or not.
    *
    * @param cacheName
    * @return <tt>true</tt> if the cache with the given name has not yet been
    *         started, or if it was started, whether it's been removed or not.
    */
   boolean cacheExists(String cacheName);

   /**
    * Retrieves a named cache from the system in the same way that {@link
    * #getCache(String)} does except that if offers the possibility for the
    * named cache not to be retrieved if it has not yet been started, or if
    * it's been removed after being started.
    *
    * @param cacheName name of cache to retrieve
    * @param createIfAbsent if <tt>false</tt>, the named cache will not be
    *        retrieved if it hasn't been retrieved previously or if it's been
    *        removed. If <tt>true</tt>, this methods works just like {@link
    *        #getCache(String)}
    * @return null if no named cache exists as per rules set above, otherwise
    *         returns a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent);

   /**
    * Starts a set of caches in parallel. Infinispan supports both symmetric
    * and asymmetric clusters; that is, multiple nodes having the same or
    * different sets of caches running, respectively. Calling this method on
    * application/application server startup with all your cache names
    * will ensure that the cluster is symmetric.
    *
    * @param cacheNames the names of the caches to start
    * @since 5.0
    */
   EmbeddedCacheManager startCaches(String... cacheNames);

   /**
    * Removes a cache with the given name from the system. This is a cluster
    * wide operation that results not only in stopping the cache with the given
    * name in all nodes in the cluster, but also deletes its contents both in
    * memory and in any backing cache store.
    *
    * @param cacheName name of cache to remove
    */
   void removeCache(String cacheName);

   /**
    * @since 5.1
    */
   Transport getTransport();

   GlobalComponentRegistry getGlobalComponentRegistry();
}
