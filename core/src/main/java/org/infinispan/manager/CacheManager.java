package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.DuplicateCacheNameException;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.notifications.Listenable;
import org.infinispan.remoting.transport.Address;

import java.util.List;

/**
 * A <tt>CacheManager</tt> is the primary mechanism for retrieving a {@link org.infinispan.Cache} instance, and is often
 * used as a starting point to using the {@link org.infinispan.Cache}.
 * <p/>
 * <tt>CacheManager</tt>s are heavyweight objects, and we foresee no more than one <tt>CacheManager</tt> being used per
 * JVM (unless specific configuration requirements require more than one; but either way, this would be a minimal and
 * finite number of instances).
 * <p/>
 * Constructing a <tt>CacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.config.Configuration} or a path or URL to a configuration XML file.
 * <p/>
 * Lifecycle - <tt>CacheManager</tt>s have a lifecycle (it implements {@link org.infinispan.lifecycle.Lifecycle}) and
 * the default constructors also call {@link #start()}.  Overloaded versions of the constructors are available, that do
 * not start the <tt>CacheManager</tt>, although it must be kept in mind that <tt>CacheManager</tt>s need to be started
 * before they can be used to create <tt>Cache</tt> instances.
 * <p/>
 * Once constructed, <tt>CacheManager</tt>s should be made available to any component that requires a <tt>Cache</tt>,
 * via <a href="http://en.wikipedia.org/wiki/Java_Naming_and_Directory_Interface">JNDI</a> or via some other mechanism
 * such as an <a href="http://en.wikipedia.org/wiki/Dependency_injection">dependency injection</a> framework.
 * <p/>
 * You obtain <tt>Cache</tt> instances from the <tt>CacheManager</tt> by using one of the overloaded
 * <tt>getCache()</tt>, methods.  Note that with <tt>getCache()</tt>, there is no guarantee that the instance you get is
 * brand-new and empty, since caches are named and shared.  Because of this, the <tt>CacheManager</tt> also acts as a
 * repository of <tt>Cache</tt>s, and is an effective mechanism of looking up or creating <tt>Cache</tt>s on demand.
 * <p/>
 * When the system shuts down, it should call {@link #stop()} on the <tt>CacheManager</tt>.  This will ensure all caches
 * within its scope are properly stopped as well.
 * <p/>
 * Sample usage: <code> CacheManager manager = CacheManager.getInstance("my-config-file.xml"); Cache entityCache =
 * manager.getCache("myEntityCache"); entityCache.put("aPerson", new Person());
 * <p/>
 * Configuration myNewConfiguration = new Configuration(); myNewConfiguration.setCacheMode(Configuration.CacheMode.LOCAL);
 * manager.defineCache("myLocalCache", myNewConfiguration); Cache localCache = manager.getCache("myLocalCache");
 * </code>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@NonVolatile
public interface CacheManager extends Lifecycle, Listenable {
   /**
    * Defines a named cache.  Named caches can be defined by using this method, in which case the configuration passed
    * in is used to override the default configuration used when this cache manager instance was created.
    * <p/>
    * The other way to define named caches is declaratively, in the XML file passed in to the cache manager.
    * <p/>
    * A combination of approaches may also be used, provided the names do not conflict.
    *
    * @param cacheName             name of cache to define
    * @param configurationOverride configuration overrides to use
    * @throws DuplicateCacheNameException if the name is already in use.
    */
   void defineCache(String cacheName, Configuration configurationOverride) throws DuplicateCacheNameException;

   /**
    * Retrieves the default cache associated with this cache manager.
    * <p/>
    * As such, this method is always guaranteed to return the default cache.
    *
    * @return the default cache.
    */
   <K, V> Cache<K, V> getCache();

   /**
    * Retrieves a named cache from the system.  If the cache has been previously created with the same name, the running
    * cache instance is returned.  Otherwise, this method attempts to create the cache first.
    * <p/>
    * When creating a new cache, this method will use the configuration passed in to the CacheManager on construction,
    * as a template, and then optionally apply any overrides previously defined for the named cache using the {@link
    * #defineCache(String, org.infinispan.config.Configuration)} method, or declared in the configuration file.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);

   /**
    * @return the name of the cluster.  Null if running in local mode.
    */
   String getClusterName();

   List<Address> getMembers();

   Address getAddress();

   boolean isCoordinator();

   ComponentStatus getStatus();
}
