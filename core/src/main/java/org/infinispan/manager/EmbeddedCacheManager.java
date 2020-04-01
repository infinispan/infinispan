package org.infinispan.manager;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.configuration.ClassWhiteList;
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
 * before they can be used to readWriteMap <tt>Cache</tt> instances.
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
public interface EmbeddedCacheManager extends CacheContainer, Listenable, Closeable {

   /**
    * Register a cache configuration in the cache manager.
    * <p/>
    * The configuration will be automatically used when creating a cache with the same name,
    * unless it is a template.
    * If it is a template and it contains wildcards (`*` or `?`), it will be automatically used
    * when creating caches that match the template.
    * <p/>
    * In order to extend an existing configuration,
    * use {@link ConfigurationBuilder#read(org.infinispan.configuration.cache.Configuration)}.
    * <p/>
    * The other way to define a cache configuration is declaratively, in the XML file passed in to the cache
    * manager.
    *
    * @param cacheName     name of the cache configuration
    * @param configuration the cache configuration
    * @return the cache configuration
    * @throws org.infinispan.commons.CacheConfigurationException if a configuration with the same name already exists.
    */
   Configuration defineConfiguration(String cacheName, Configuration configuration);

   /**
    * Defines a cache configuration by first reading the template configuration and then applying the override.
    * <p/>
    * The configuration will be automatically used when creating a cache with the same name,
    * unless it is a template.
    * If it is a template and it contains wildcards (`*` or `?`), it will be automatically used
    * when creating caches that match the template.
    * <p/>
    * The other way to define a cache configuration is declaratively, in the XML file passed in to the cache
    * manager.
    * <p/>
    * If templateName is null, this method works exactly like {@link #defineConfiguration(String, Configuration)}.
    *
    * @param cacheName             name of cache whose configuration is being defined
    * @param templateCacheName     configuration to use as a template
    * @param configurationOverride configuration overrides on top of the template
    * @return the configuration
    * @throws org.infinispan.commons.CacheConfigurationException if a configuration with the same name already exists.
    */
   Configuration defineConfiguration(String cacheName, String templateCacheName, Configuration configurationOverride);

   /**
    * Removes a configuration from the set of defined configurations.
    * <p/>
    * If the named configuration does not exist, nothing happens.
    *
    * @param configurationName     the named configuration
    * @throws IllegalStateException if the configuration is in use
    */
   void undefineConfiguration(String configurationName);

   /**
    * @return the name of the cluster.  Null if running in local mode.
    */
   String getClusterName();

   /**
    * @return the addresses of all the members in the cluster, or {@code null} if not connected
    */
   List<Address> getMembers();

   /**
    * Warning: the address may be {@code null} before the first clustered cache starts
    * and after all the clustered caches have been stopped.
    *
    * @return the address of the local node, or {@code null} if not connected
    */
   Address getAddress();

   /**
    * @return the address of the cluster's coordinator, or {@code null} if not connected
    */
   Address getCoordinator();

   /**
    * @return whether the local node is the cluster's coordinator, or {@code null} if not connected
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
    * @return the default cache's configuration, or {@code null} if there is no default cache.
    */
   org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration();

   /**
    * This method returns a collection of all cache configuration names.
    * <p/>
    * The configurations may have been defined via XML,
    * programmatically via {@link org.infinispan.configuration.parsing.ConfigurationBuilderHolder},
    * or at runtime via {@link #defineConfiguration(String, Configuration)}.
    * <p/>
    * Internal caches defined via {@link org.infinispan.registry.InternalCacheRegistry}
    * are not included.
    *
    * @return an immutable set of configuration names registered in this cache manager.
    *
    * @since 8.2
    */
   default Set<String> getCacheConfigurationNames() {
      throw new UnsupportedOperationException();
   }

   /**
    * Tests whether a cache is running.
    * @param cacheName name of cache to test.
    * @return true if the cache exists and is running; false otherwise.
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
    * @param cacheName cache to check
    * @return <tt>true</tt> if the cache with the given name has not yet been
    *         started, or if it was started, whether it's been removed or not.
    */
   boolean cacheExists(String cacheName);

   /**
    * Retrieves the default cache associated with this cache container.
    *
    * @return the default cache.
    * @throws org.infinispan.commons.CacheConfigurationException if a default cache does not exist.
    */
   <K, V> Cache<K, V> getCache();

   /**
    * Retrieves a cache by name.
    * <p/>
    * If the cache has been previously created with the same name, the running
    * cache instance is returned.
    * Otherwise, this method attempts to create the cache first.
    * <p/>
    * When creating a new cache, this method requires a defined configuration that either has exactly the same name,
    * or is a template with wildcards and matches the cache name.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);

   /**
    * Creates a cache on the local node using the supplied configuration.
    * <p/>
    * The cache may be clustered, but this method (or the equivalent combination of
    * {@link #defineConfiguration(String, Configuration)} and
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
    * Similar to {@link #getCache(String)}, except if has the option
    * to not create the cache if it is not already running.
    *
    * @param cacheName name of cache to retrieve
    * @param createIfAbsent If <tt>true</tt>, this methods works just like {@link #getCache(String)}.
    *                       If <tt>false</tt>, return the already running cache or <tt>null</tt>.
    * @return <tt>null</tt> if the cache does not exist and <tt>createIfAbsent == false</tt>,
    *        otherwise a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent);

   /**
    * Similar to {@link #getCache(String)}, only with an explicit configuration template name.
    * <p/>
    * Multiple caches can be created using the same configuration.
    *
    * @param cacheName name of cache to retrieve
    * @param configurationName name of the configuration template to use
    * @return a cache instance identified by cacheName
    * @throws org.infinispan.commons.CacheConfigurationException if the configuration does not exist
    *         or if a configuration named <tt>cacheName</tt> already exists.
    * @deprecated as of 9.0. Use {@link EmbeddedCacheManager#defineConfiguration(String, String, Configuration)} and
    * {@link EmbeddedCacheManager#getCache(String)} instead
    */
   <K, V> Cache<K, V> getCache(String cacheName, String configurationName);

   /**
    * Similar to {@link #getCache(String, boolean)}, only with an explicit configuration template name.
    * <p/>
    * Multiple caches can be created using the same configuration.
    *
    * @param cacheName name of cache to retrieve
    * @param configurationTemplate name of the configuration template to use
    * @param createIfAbsent If <tt>true</tt>, this methods works just like {@link #getCache(String)}.
    *                       If <tt>false</tt>, return the already running cache or <tt>null</tt>.
    * @return <tt>null</tt> if the cache does not exist and <tt>createIfAbsent == false</tt>,
    *        otherwise a cache instance identified by cacheName
    * @throws org.infinispan.commons.CacheConfigurationException if the configuration does not exist
    *         or if a configuration named <tt>cacheName</tt> already exists.
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
    * @deprecated Since 9.2, obtain a {@link org.infinispan.commons.api.CacheContainerAdmin} instance using {@link #administration()} and invoke the {@link org.infinispan.commons.api.CacheContainerAdmin#removeCache(String)} method
    */
   @Deprecated
   void removeCache(String cacheName);

   /**
    * @deprecated Since 10.0, please use {@link #getAddress()}, {@link #getMembers()}, {@link #getCoordinator()}
    */
   Transport getTransport();

   /**
    * @deprecated Since 10.0, with no public API replacement
    */
   @Deprecated
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
    * @since 7.1
    * @return statistics for this cache manager
    * @deprecated Since 10.1.3. This mixes statistics across unrelated caches so the reported numbers don't have too much
    * relevance.
    */
   @Deprecated
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
    * @return an instance of {@link CacheManagerInfo} used to get basic info about the cache manager.
    */
   CacheManagerInfo getCacheManagerInfo();

   /**
    * Provides an {@link EmbeddedCacheManagerAdmin} whose methods affect the entire cluster as opposed to a single node.
    *
    * @since 9.2
    * @return a cluster-aware {@link EmbeddedCacheManagerAdmin}
    */
   default EmbeddedCacheManagerAdmin administration() {
      throw new UnsupportedOperationException();
   }

   ClassWhiteList getClassWhiteList();

   Subject getSubject();

   EmbeddedCacheManager withSubject(Subject subject);
}
