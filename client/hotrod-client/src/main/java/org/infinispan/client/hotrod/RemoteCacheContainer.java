package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.Marshaller;

import java.util.Properties;

public interface RemoteCacheContainer extends BasicCacheContainer {

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

   /**
    * Retrieves a clone of the properties currently in use.  Note that making
    * any changes to the properties instance retrieved will not affect an
    * already-running RemoteCacheManager.
    *
    * @return a clone of the properties used to configure this RemoteCacheManager
    */
   @Deprecated
   Properties getProperties();

   <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue);

   <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue);

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
    * Switch remote cache manager to a the default cluster, previously
    * declared via configuration. If the switch was completed successfully,
    * this method returns {@code true}, otherwise it returns {@code false}.
    *
    * @return {@code true} if the cluster was switched, {@code false} otherwise
    */
   boolean switchToDefaultCluster();

   Marshaller getMarshaller();
}
