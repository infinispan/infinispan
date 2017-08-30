package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.ClusterContainerStats;

/**
 * Cache manager operations which affect the whole cluster. An instance of this can be retrieved from
 * {@link EmbeddedCacheManager#cluster()}
 *
 * @author Tristan Tarrant
 * @since 9.1
 */

public interface ClusterCacheManager {

   /**
    * Creates a cache across the cluster. The cache will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the cache to be created on all nodes before
    * returning.
    *
    * @param name the name of the cache
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @param <K> the generic type of the key
    * @param <V> the generic type of the value
    * @return the cache
    */
   <K, V> Cache<K, V> createCache(String name, Configuration configuration);

   /**
    * Rmoves a cache across the cluster.
    *
    * @param name the name of the cache to remove
    */
   void removeCache(String name);

   /**
    * Providess the cache manager based executor.  This can be used to execute a given operation upon the
    * cluster or a single node if desired.  If this manager is not clustered this will execute locally only.
    * <p>
    * Note that not all {@link EmbeddedCacheManager} implementations may implement this.  Those that don't will throw
    * a {@link UnsupportedOperationException} upon invocation.
    * @return an instance of {@link ClusterExecutor}
    */
   ClusterExecutor executor();

   /**
    * Returns cluster-wide container statistics
    *
    * @return an instance of {@link ClusterContainerStats}
    */
   ClusterContainerStats getStats();

}
