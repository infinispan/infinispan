package org.infinispan.manager;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Set;

/**
 * EmbeddedCacheManager is an CacheManager that runs in the same JVM as the client.
 * <p/>
 * Constructing a <tt>EmbeddedCacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.config.Configuration} or a path or URL to a configuration XML file: see {@link org.infinispan.manager.DefaultCacheManager}.
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
    * If cache name hasn't been defined before, this method creates a clone of the default cache's configuration,
    * applies a clone of the configuration overrides passed in and returns this configuration instance.
    * <p/>
    * If cache name has been previously defined, this method creates a clone of this cache's existing configuration,
    * applies a clone of the configuration overrides passed in and returns the configuration instance.
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
   GlobalConfiguration getGlobalConfiguration();

   /**
    * Returns default configuration for this CacheManager
    *
    * @return the default configuration associated with this CacheManager
    */
   Configuration getDefaultConfiguration();

   /**
    * If no named caches are registered, this method returns an empty set.  The default cache is never included in this
    * set of cache names.
    *
    * @return an immutable set of non-default named caches registered with this cache manager.
    */
   Set<String> getCacheNames();
}
