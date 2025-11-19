package org.infinispan.jcache.remote;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.GlobalContextInitializer;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheManager;
import org.infinispan.jcache.logging.Log;
import org.infinispan.protostream.SerializationContext;

public class JCacheManager extends AbstractJCacheManager {
   private static final Log log = Log.getLog(JCacheManager.class);

   private final RemoteCacheManager cm;

   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider, Properties userProperties) {
      super(uri, classLoader, provider, userProperties, false);

      ConfigurationBuilder builder = getConfigurationBuilder(uri, userProperties);
      builder.forceReturnValues(true);

      org.infinispan.client.hotrod.configuration.Configuration configuration = builder.build();
      cm = new RemoteCacheManager(configuration, true);
      initializeProtoContext(cm.getMarshaller());
   }

   private void initializeProtoContext(Marshaller marshaller) {
      if (marshaller instanceof ProtoStreamMarshaller) {
         SerializationContext ctx = ((ProtoStreamMarshaller) marshaller).getSerializationContext();
         GlobalContextInitializer.INSTANCE.register(ctx);
      }
   }

   private ConfigurationBuilder getConfigurationBuilder(URI uri, Properties userProperties) {
      ConfigurationBuilder builder = new ConfigurationBuilder();

      // Attempt to load the URI as a Hot Rod properties file
      try (InputStream is = uri.toURL().openStream()) {
         Properties properties = new Properties();
         properties.load(is);
         // Merge all of the user properties which will override any properties in the config file
         if (userProperties != null) {
            properties.putAll(userProperties);
         }
         builder.withProperties(properties);
         this.properties = properties;
         return builder;
      } catch (IllegalArgumentException | MalformedURLException e) {
         // The URI did not point to a hotrod-client.properties file, fall-through and use other strategies
      } catch (IOException e) {
         throw new CacheException("Could not load configuration", e);
      }

      // See if there is a hotrod-client.properties file in the classpath first
      try (InputStream is = findPropertiesFile()) {
         Properties properties = new Properties();
         // Load only if found
         if (is != null) {
            properties.load(is);
         }
         // Merge all of the user properties which will override any properties in the config file
         if (userProperties != null) {
            properties.putAll(userProperties);
         }
         this.properties = properties;
         return builder.withProperties(properties);
      } catch (IOException e) {
         throw new CacheException("Could not load configuration", e);
      }
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
      cm.administration().getOrCreateCache(cacheName, (String) null); // TODO: ISPN-9237 convert a JCache configuration to an Infinispan XML
      return createJCache(cacheName, adapter);
   }

   private <K, V> AbstractJCache<K, V> createJCache(String cacheName, ConfigurationAdapter<K, V> adapter) {
      RemoteCache<K, V> ispnCache = getRemoteCache(cm, cacheName);
      RemoteCache<K, V> ispnCacheForceReturnValue = ispnCache.withFlags(Flag.FORCE_RETURN_VALUE);
      return new JCache<>(ispnCache, ispnCacheForceReturnValue, this, adapter);
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
      if (ispnCache != null) {
         RemoteCache<K, V> ispnCacheForceReturnValue = ispnCache.withFlags(Flag.FORCE_RETURN_VALUE);
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
   }

   @Override
   protected boolean delegateIsClosed() {
      return false;
      //FIXME implement me
   }

   @Override
   protected <K, V> void delegateRemoveCache(AbstractJCache<K, V> jcache) {
      cm.administration().removeCache(jcache.getName());
   }

   @Override
   protected void delegateLogIsClosed() {
      throw log.cacheClosed();
   }
}
