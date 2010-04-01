package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.HotrodOperations;
import org.infinispan.client.hotrod.impl.HotrodOperationsImpl;
import org.infinispan.client.hotrod.impl.HotrodMarshaller;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.SerializationMarshaller;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class RemoteCacheManager implements CacheContainer, Lifecycle {

   private Properties props;
   private TransportFactory transportFactory;
   private String hotrodMarshaller;


   /**
    * Build a cache manager based on supplied given properties.
    * TODO - add a list of all possible configuration parameters here
    */
   public RemoteCacheManager(Properties props, boolean start) {
      this.props = props;
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties, boolean)}, and it also starts the cache (start==true).
    */
   public RemoteCacheManager(Properties props) {
      this.props = props;
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in the
    * classpath, in a file named <tt>hotrod-client.properties</tt>.
    * @param start weather or not to start the RemoteCacheManager
    * @throws HotRodClientException if such a file cannot be found in the classpath
    */
   public RemoteCacheManager(boolean start) {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      InputStream stream = loader.getResourceAsStream("hotrod-client.properties");
      loadFromStream(stream);
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(boolean)} and it also starts the cache.
    */
   public RemoteCacheManager() {
      this(true);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in
    * supplied URL.
    * @param start weather or not to start the RemoteCacheManager 
    * @throws HotRodClientException if properties could not be loaded
    */
   public RemoteCacheManager(URL config, boolean start) {
      try {
         loadFromStream(config.openStream());
      } catch (IOException e) {
         throw new HotRodClientException("Could not read URL:" + config, e);
      }
      if (start)
         start();
   }

   /**
    * Same as {@link #RemoteCacheManager(java.net.URL)} and it also starts the cache (start==true).
    * @param config
    */
   public RemoteCacheManager(URL config) {
      this(config, true);
   }
   

   private void loadFromStream(InputStream stream) {
      props = new Properties();
      try {
         props.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties",e);
      }
   }

   public <K,V> RemoteCache<K,V> getCache(String cacheName) {
      return createRemoteCache(cacheName);
   }

   public <K,V> RemoteCache<K,V> getCache() {
      return createRemoteCache(DefaultCacheManager.DEFAULT_CACHE_NAME);
   }

   @Override
   public void start() {
      String factory = props.getProperty("transport-factory"); //todo switch to Netty transport by default (second param)
      if (factory == null)
         throw new IllegalStateException("Missing required property: transport-factory!");
      transportFactory = (TransportFactory) newInstance(factory);
      transportFactory.init(props);
      hotrodMarshaller = props.getProperty("marshaller", SerializationMarshaller.class.getName());
   }

   @Override
   public void stop() {
      transportFactory.destroy();
   }

   private Object newInstance(String clazz) {
      try {
         return Util.getInstance(clazz);
      } catch (Exception e) {
         throw new HotRodClientException("Could not instantiate class: " + clazz, e);
      }
   }

   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName) {
      HotrodMarshaller marshaller = (HotrodMarshaller) newInstance(hotrodMarshaller);
      HotrodOperations hotrodOperations = new HotrodOperationsImpl(cacheName, transportFactory);
      return new RemoteCacheImpl<K, V>(hotrodOperations, marshaller, cacheName);
   }
}
