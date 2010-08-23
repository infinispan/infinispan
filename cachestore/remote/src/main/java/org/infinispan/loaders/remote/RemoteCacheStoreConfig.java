package org.infinispan.loaders.remote;

import org.infinispan.CacheException;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.FileLookup;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration for RemoteCacheStore.
 * <p/>
 * Parameters:
 * <ul>
 * <li>HotRodClientPropertiesFile-the file that contains the configuration of Hot Rod client. See <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 *     for more details on the Hot Rod client. 
 * <li>remoteCacheName-the name of the remote cache in the remote infinispan cluster, to which to connect to</li>
 * <li>UseDefaultRemoteCache-if set to true, the default remote cache will be used, as obtained by {@link org.infinispan.manager.CacheContainer#getCache()}.
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheStoreConfig extends AbstractCacheStoreConfig {

   private volatile String remoteCacheName;

   private final Properties hotRodClientProperties = new Properties();

   public RemoteCacheStoreConfig() {
      setCacheLoaderClassName(RemoteCacheStore.class.getName());
   }

   public void setRemoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
   }

   public String getRemoteCacheName() {
      return remoteCacheName;
   }

   public void setUseDefaultRemoteCache(boolean useDefaultRemoteCache) {
      if (useDefaultRemoteCache) {
         setRemoteCacheName(CacheContainer.DEFAULT_CACHE_NAME);
      }
   }

   public boolean isUseDefaultRemoteCache() {
      return CacheContainer.DEFAULT_CACHE_NAME.equals(getRemoteCacheName());
   }

   public Properties getHotRodClientProperties() {
      return hotRodClientProperties;
   }

   public void setHotRodClientProperties(Properties props) {
      this.hotRodClientProperties.putAll(props);
   }

   public void setHotRodClientPropertiesFile(String hotRodClientProperties) {
      FileLookup fileLookup = new FileLookup();
      InputStream inputStream = fileLookup.lookupFile(hotRodClientProperties);
      try {
         this.hotRodClientProperties.load(inputStream);
      } catch (IOException e) {
         log.error("Issues while loading properties from file", e);
         throw new CacheException(e);
      }
   }
}
