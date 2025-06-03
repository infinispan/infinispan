package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.Marshaller;

public interface RemoteCacheContainer extends BasicCacheContainer {

   /**
    * @see BasicCacheContainer#getCache()
    */
   @Override
   <K, V> RemoteCache<K, V> getCache();

   /**
    * @see BasicCacheContainer#getCache(String)
    */
   @Override
   <K, V> RemoteCache<K, V> getCache(String cacheName);

   /**
    * Retrieves the configuration currently in use. The configuration object
    * is immutable. If you wish to change configuration, you should use the
    * following pattern:
    *
    * <pre><code>
    * ConfigurationBuilder builder = new ConfigurationBuilder();
    * builder.read(remoteCacheManager.getConfiguration());
    * // modify builder
    * remoteCacheManager.stop();
    * remoteCacheManager = new RemoteCacheManager(builder.build());
    * </code></pre>
    *
    * @return The configuration of this RemoteCacheManager
    */
   Configuration getConfiguration();

   boolean isStarted();

   /**
    * Switch remote cache manager to a different cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @param clusterName name of the cluster to which to switch to
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToCluster(String clusterName);

   /**
    * Switch remote cache manager to the default cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToDefaultCluster();

   /**
    * Returns the name of the currently active cluster.
    *
    * @return the name of the active cluster
    */
   String getCurrentClusterName();

   Marshaller getMarshaller();

   /**
    * @return {@code true} if the cache with name {@code cacheName} can participate in transactions.
    */
   boolean isTransactional(String cacheName);
}
