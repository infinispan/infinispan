package org.infinispan.jcache.remote;

import java.net.URI;
import java.util.Properties;

import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheManager;
import org.infinispan.jcache.logging.Log;
import org.infinispan.jcache.remote.ServerManager.ManagementClientException;
import org.infinispan.jcache.remote.ServerManager.NotAvailableException;
import org.infinispan.commons.logging.LogFactory;

public class JCacheManager extends AbstractJCacheManager {
   private static final Log log = LogFactory.getLog(JCacheManager.class, Log.class);

   private RemoteCacheManager cm;
   private RemoteCacheManager cmForceReturnValue;

   private ServerManager sm;

   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties properties) {
      super(uri, classLoader, provider, properties, false);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (properties != null) {
         builder.withProperties(properties);
      } else {
         builder.addServer().host("127.0.0.1").port(11222);
      }

      org.infinispan.client.hotrod.configuration.Configuration configuration = builder.build();
      cm = new RemoteCacheManager(configuration, true);

      builder.forceReturnValues(true);
      cmForceReturnValue = new RemoteCacheManager(builder.build(), true);

      sm = new ServerManager(configuration.servers().get(0).host());
   }

   @Override
   public ClassLoader getClassLoader() {
      return cm.getConfiguration().classLoader();
   }

   @Override
   protected <K, V, C extends Configuration<K, V>> AbstractJCache<K, V> create(String cacheName, C configuration) {
      ConfigurationAdapter<K, V> adapter = ConfigurationAdapter.create(configuration);

      try {
         if (!sm.containsCache(cacheName)) {
            sm.addCache(cacheName);
         }
      } catch (NotAvailableException ex) {
         //Nothing to do.
      } catch (ManagementClientException ex) {
         throw log.cacheCreationFailed(cacheName, ex);
      }

      RemoteCache<K, V> ispnCache = cm.getCache(cacheName);
      if (ispnCache == null) {
         throw log.cacheNotFound(cacheName);
      }

      RemoteCache<K, V> ispnCacheForceReturnValue = cmForceReturnValue.getCache(cacheName);
      if (ispnCacheForceReturnValue == null) {
         throw log.cacheNotFound(cacheName);
      }

      return new JCache<K, V>(ispnCache, ispnCacheForceReturnValue, this, adapter);
   }

   @Override
   protected <K, V, I extends BasicCache<K, V>> AbstractJCache<K, V> create(I ispnCache) {
      return null;
      //FIXME implement me
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   @Override
   protected Iterable<String> delegateCacheNames() {
      return getManagedCacheNames();
   }

   @Override
   protected void delegateStop() {
      cm.stop();
      cmForceReturnValue.stop();
   }

   @Override
   protected boolean delegateIsClosed() {
      return false;
      //FIXME implement me
   }

   @Override
   protected <K, V> void delegateRemoveCache(AbstractJCache<K, V> jcache) {
      try {
         sm.removeCache(jcache.getName());
      } catch (NotAvailableException ex) {
         // Nothing to do.
      } catch (ManagementClientException ex) {
         throw log.serverManagementOperationFailed(ex);
      }
      jcache.close();
   }

   @Override
   protected void delegateLogIsClosed() {
      throw log.cacheClosed();
   }
}
