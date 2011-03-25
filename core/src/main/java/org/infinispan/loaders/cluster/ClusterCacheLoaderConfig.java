package org.infinispan.loaders.cluster;

import org.infinispan.loaders.AbstractCacheLoaderConfig;

/**
 * Configuration for {@link org.infinispan.loaders.cluster.ClusterCacheLoader}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ClusterCacheLoaderConfig extends AbstractCacheLoaderConfig {

   private static final long serialVersionUID = -44748358960849539L;
   
   private long remoteCallTimeout;

   public String getCacheLoaderClassName() {
      return ClusterCacheLoader.class.getName();
   }

   public long getRemoteCallTimeout() {
      return remoteCallTimeout;
   }

   /**
    * @deprecated The visibility of this will be reduced, {@link #remoteCallTimeout(long)}
    */
   @Deprecated
   public void setRemoteCallTimeout(long remoteCallTimeout) {
      testImmutability("remoteCallTimeout");
      this.remoteCallTimeout = remoteCallTimeout;
   }

   public ClusterCacheLoaderConfig remoteCallTimeout(long remoteCallTimeout) {
      setRemoteCallTimeout(remoteCallTimeout);
      return this;
   }
}
