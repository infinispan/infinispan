package org.infinispan.loader.cluster;

import org.infinispan.loader.AbstractCacheLoaderConfig;

/**
 * Configuration for {@link org.infinispan.loader.cluster.ClusterCacheLoader}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoaderConfig extends AbstractCacheLoaderConfig {

   private long remoteCallTimeout;

   public String getCacheLoaderClassName() {
      return ClusterCacheLoader.class.getName();
   }

   public long getRemoteCallTimeout() {
      return remoteCallTimeout;
   }

   public void setRemoteCallTimeout(long remoteCallTimeout) {
      testImmutability("remoteCallTimeout");
      this.remoteCallTimeout = remoteCallTimeout;
   }
}
