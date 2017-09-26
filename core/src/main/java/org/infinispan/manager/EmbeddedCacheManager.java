package org.infinispan.manager;

import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.health.Health;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stats.CacheContainerStats;

/**
 * EmbeddedCacheManager is an CacheManager that runs in the same JVM as the client.
 * <p/>
 * Constructing a <tt>EmbeddedCacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.configuration.cache.Configuration} or a path or URL to a configuration XML file: see {@link org.infinispan.manager.DefaultCacheManager}.
 * <p/>
 * Lifecycle - <tt>EmbeddedCacheManager</tt>s have a lifecycle (it implements {@link Lifecycle}) and
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
    * Defines a named cache's configuration by using the provided configuration
    * <p/>
    * Unlike previous versions of Infinispan, this method does not build on an existing configuration (default or named).
    * If you want this behavior, then use {@link ConfigurationBuilder#read(org.infinispan.configuration.cache.Configuration)}.
    * <p/>
    * The other way to define named cache's configuration is declaratively, in the XML file passed in to the cache
    * manager.
    * <p/>
    * If this cache was already configured either declaritively or programmatically this method will throw a
    * {@link org.infinispan.commons.CacheConfigurationException}.
    * @param cacheName             name of cache whose configuration is being defined
    * @param configuration configuration overrides to use
    * @return a cloned configuration instance
    */
   Configuration defineConfiguration(String cacheName, Configuration configuration);

   /**
    * Defines a named cache's configuration using by first reading the template configuration and then applying
    * the override afterwards to generate a configuration.
    * <p/>
    * The other way to define named cache's configuration is declaratively, in the XML file passed in to the cache
    * manager.
    * <p/>
    * If templateName is null or there isn't any named cache with that name, this methods works exactly like {@link
    * #defineConfiguration(String, Configuration)}.
    * <p/>
    * If this cache was already configured either declaritively or programmatically this method will throw a
    * {@link org.infinispan.commons.CacheConfigurationException}.
    * @param cacheName             name of cache whose configuration is being defined
    * @param templateCacheName     name of cache to use as a template before overrides are applied to it
    * @param configurationOverride configuration overrides to use
    * @return a cloned configuration instance
    */
   Configuration defineConfiguration(String cacheName, String templateCacheName, Configuration configurationOverride);

   /**
    * Removes a configuration from the set of defined configurations. If the configuration is currently in use by one of the
    * caches, an {@link IllegalStateException} is thrown. If the named configuration does not exist, nothing
    * happens
    *
    * @param configurationName     the named configuration
    */
   void undefineConfiguration(String configurationName);

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
    * set of cache names, as well a caches for internal-only use {@link org.infinispan.registry.InternalCacheRegistry}
    *
    * @return an immutable set of non-default named caches registered or
    * created with this cache manager.
    */
   Set<String> getCacheNames();

   /**
    * This method returns a collection of cache configuration names which contains the
    * cache configurations that have been defined via XML or programmatically, and the
    * cache configurations that have been defined at runtime via this cache manager
    * instance.
    *
    * If no cache configurations are registered or no caches have been created, this
    * method returns an empty set.  The default cache is never included in this
    * set of cache names, as well a caches for internal-only use {@link org.infinispan.registry.InternalCacheRegistry}
    *
    * @return an immutable set of non-default named caches registered or
    * created with this cache manager.
    *
    * @since 8.2
    */
   default Set<String> getCacheConfigurationNames() {
      throw new UnsupportedOperationException();
   }

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
    * Creates a cache on the local node using the supplied configuration. The cache may be clustered, but this
    * method (or an equivalent combinatio of {@link #defineConfiguration(String, Configuration)} and
    * {@link #getCache(String, boolean)}) needs to be invoked on all nodes.
    *
    * @param name the name of the cache
    * @param configuration the configuration to use.
    * @param <K> the generic type of the key
    * @param <V> the generic type of the value
    * @return the cache
    */
   <K, V> Cache<K, V> createCache(String name, Configuration configuration);

   /**
    * Retrieves a named cache from the system in the same way that {@link
    * #getCache(String)} does except that if offers the possibility for the
    * named cache not to be retrieved if it has not yet been started, or if
    * it's been removed after being started. If a non-template configuration
    * exists with the same name, it will be used to configure the cache.
    *
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
    * Retrieves a named cache from the system in the same way that {@link
    * #getCache(String)} does except that if offers the possibility to specify
    * a specific configuration template. Multiple caches can be created using
    * the same configuration.
    *
    * @param cacheName name of cache to retrieve
    * @param configurationName name of the configuration template to use
    * @return null if no configuration exists as per rules set above, otherwise
    *         returns a cache instance identified by cacheName
    * @deprecated as of 9.0. Use {@link EmbeddedCacheManager#defineConfiguration(String, String, Configuration)} and
    * {@link EmbeddedCacheManager#getCache(String)} instead
    */
   <K, V> Cache<K, V> getCache(String cacheName, String configurationName);

   /**
    * Retrieves a named cache from the system in the same way that {@link
    * #getCache(String)} does except that if offers the possibility to specify
    * a specific configuration template. Multiple caches can be created using
    * the same configuration. Tihs method also offers the possibility for the
    * named cache not to be retrieved if it has not yet been started, or if
    * it's been removed after being started.
    *
    * @param cacheName name of cache to retrieve
    * @param configurationTemplate name of the configuration template to use
    * @param createIfAbsent if <tt>false</tt>, the named cache will not be
    *        retrieved if it hasn't been retrieved previously or if it's been
    *        removed. If <tt>true</tt>, this methods works just like {@link
    *        #getCache(String, String)}
    * @return null if no configuration exists as per rules set above, otherwise
    *         returns a cache instance identified by cacheName
    * @deprecated as of 9.0. Use {@link EmbeddedCacheManager#defineConfiguration(String, String, Configuration)} and
    * {@link EmbeddedCacheManager#getCache(String, boolean)} instead
    */
   <K, V> Cache<K, V> getCache(String cacheName, String configurationTemplate, boolean createIfAbsent);

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

   /**
    * Add a dependency between two caches. The cache manager will make sure that
    * a cache is stopped before any of its dependencies
    *
    * @param from cache name
    * @param to cache name
    * @since 7.0
    */
   void addCacheDependency(String from, String to);

   /**
    * Returns statistics for this cache manager
    *
    * since 7.1
    * @return statistics for this cache manager
    */
   CacheContainerStats getStats();

   /**
    * Providess the cache manager based executor.  This can be used to execute a given operation upon the
    * cluster or a single node if desired.  If this manager is not clustered this will execute locally only.
    * <p>
    * Note that not all {@link EmbeddedCacheManager} implementations may implement this.  Those that don't will throw
    * a {@link UnsupportedOperationException} upon invocation.
    * @return
    */
   default ClusterExecutor executor() {
      throw new UnsupportedOperationException();
   }

   /**
    * Returns an entry point for a Health Check API.
    *
    * @since 9.0
    * @return Health API for this {@link EmbeddedCacheManager}.
     */
   Health getHealth();

   /**
    * Provides an {@link EmbeddedCacheManager} whose methods affect the entire cluster as opposed to a single node.
    *
    * @since 9.1
    * @return a cluster-aware {@link EmbeddedCacheManager}
    */
   default ClusterCacheManager cluster() {
      throw new UnsupportedOperationException();
   }
}
