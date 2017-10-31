package org.infinispan.jcache.remote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheManager;
import org.infinispan.jcache.logging.Log;

public class JCacheManager extends AbstractJCacheManager {
   private static final Log log = LogFactory.getLog(JCacheManager.class, Log.class);

   private RemoteCacheManager cm;
   private RemoteCacheManager cmForceReturnValue;

   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties properties) {
      super(uri, classLoader, provider, properties, false);

      ConfigurationBuilder builder = getConfigurationBuilder(properties);

      org.infinispan.client.hotrod.configuration.Configuration configuration = builder.build();
      cm = new RemoteCacheManager(configuration, true);

      builder.forceReturnValues(true);
      cmForceReturnValue = new RemoteCacheManager(builder.build(), true);
   }

   private ConfigurationBuilder getConfigurationBuilder(Properties userProperties) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      InputStream is;
      if (userProperties != null && !userProperties.isEmpty()) {
         this.properties = userProperties;
         builder.withProperties(userProperties);
      } else if ((is = findPropertiesFile()) != null) {
         Properties fileProperties = new Properties();
         try {
            fileProperties.load(is);
            this.properties = fileProperties;
            builder.withProperties(fileProperties);
         } catch (IOException e) {
            throw new CacheException("Unable to load properties from `hotrod-client.properties`", e);
         }
      } else {
         builder.addServer().host("127.0.0.1").port(11222);
      }
      return builder;
   }

   private InputStream findPropertiesFile() {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      return FileLookupFactory.newInstance()
            .lookupFile(RemoteCacheManager.HOTROD_CLIENT_PROPERTIES, cl);
   }

   @Override
   public ClassLoader getClassLoader() {
      return cm.getConfiguration().classLoader();
   }

   @Override
   protected <K, V, C extends Configuration<K, V>> AbstractJCache<K, V> create(String cacheName, C configuration) {
      ConfigurationAdapter<K, V> adapter = ConfigurationAdapter.create(configuration);
      RemoteCache<Object, Object> existing = cm.getCache(cacheName);
      if (existing == null) {
         cm.administration().createCache(cacheName, null);
      }
      return createJCache(cacheName, adapter);
   }

   private <K, V> AbstractJCache<K, V> createJCache(String cacheName, ConfigurationAdapter<K, V> adapter) {
      RemoteCache<K, V> ispnCache = getRemoteCache(cm, cacheName);
      RemoteCache<K, V> ispnCacheForceReturnValue = getRemoteCache(cmForceReturnValue, cacheName);
      return new JCache<K, V>(ispnCache, ispnCacheForceReturnValue, this, adapter);
   }

   private <K, V> RemoteCache<K, V> getRemoteCache(RemoteCacheManager cm, String cacheName) {
      RemoteCache<K, V> ispnCache = cm.getCache(cacheName);
      if (ispnCache == null) {
         throw log.cacheNotFound(cacheName);
      }
      return ispnCache;
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      Cache<K, V> cache = super.getCache(cacheName);
      return Objects.isNull(cache)
            ? createRegisterJCache(cacheName)
            : cache;
   }

   public <K, V> Cache<K, V> createRegisterJCache(String cacheName) {
      RemoteCache<K, V> ispnCache = cm.getCache(cacheName);
      RemoteCache<K, V> ispnCacheForceReturnValue = cmForceReturnValue.getCache(cacheName);
      if (ispnCache != null && cmForceReturnValue != null) {
         ConfigurationAdapter<K, V> adapter = ConfigurationAdapter.create();
         JCache<K, V> jcache = new JCache<>(ispnCache, ispnCacheForceReturnValue, this, adapter);
         registerPredefinedCache(cacheName, jcache);
         return jcache;
      }
      return null;
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
      Cache<K, V> cache = super.getCache(cacheName, keyType, valueType);
      return Objects.isNull(cache)
            ? createRegisterJCache(cacheName)
            : cache;
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
      cm.administration().removeCache(jcache.getName());
      jcache.close();
   }

   @Override
   protected void delegateLogIsClosed() {
      throw log.cacheClosed();
   }
}
