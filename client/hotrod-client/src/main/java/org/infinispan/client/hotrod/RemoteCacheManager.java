package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.counter.impl.RemoteCounterManager;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.RemoteCacheManagerAdminImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation.PingResult;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.CodecFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.uberjar.ManifestUberJarDuplicatedJarsWarner;
import org.infinispan.commons.util.uberjar.UberJarDuplicatedJarsWarner;
import org.infinispan.counter.api.CounterManager;

/**
 * <p>Factory for {@link org.infinispan.client.hotrod.RemoteCache}s.</p>
 * <p>In order to be able to use a {@link org.infinispan.client.hotrod.RemoteCache}, the
 * {@link org.infinispan.client.hotrod.RemoteCacheManager} must be started first: this instantiates connections to
 * Hot Rod server(s). Starting the {@link org.infinispan.client.hotrod.RemoteCacheManager} can be done either at
 * creation by passing start==true to the constructor or by using a constructor that does that for you; or after
 * construction by calling {@link #start()}.</p>
 * <p><b>NOTE:</b> this is an "expensive" object, as it manages a set of persistent TCP connections to the Hot Rod
 * servers. It is recommended to only have one instance of this per JVM, and to cache it between calls to the server
 * (i.e. remoteCache operations)</p>
 * <p>{@link #stop()} needs to be called explicitly in order to release all the resources (e.g. threads,
 * TCP connections).</p>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheManager implements RemoteCacheContainer, Closeable {

   private static final Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String DEFAULT_CACHE_NAME = "___defaultcache";
   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";
   public static final String JSON_STRING_ARRAY_ELEMENT_REGEX = "(?:\")([^\"]*)(?:\",?)";

   private volatile boolean started = false;
   private final Map<RemoteCacheKey, RemoteCacheHolder> cacheName2RemoteCache = new HashMap<>();
   private final AtomicInteger defaultCacheTopologyId = new AtomicInteger(HotRodConstants.DEFAULT_CACHE_TOPOLOGY);
   private Configuration configuration;
   private Codec codec;

   private Marshaller marshaller;
   protected ChannelFactory channelFactory;
   protected ClientListenerNotifier listenerNotifier;
   private final Runnable start = this::start;
   private final Runnable stop = this::stop;
   private final RemoteCounterManager counterManager;

   /**
    * Create a new RemoteCacheManager using the supplied {@link Configuration}.
    * The RemoteCacheManager will be started automatically
    *
    * @param configuration the configuration to use for this RemoteCacheManager
    * @since 5.3
    */
   public RemoteCacheManager(Configuration configuration) {
      this(configuration, true);
   }

   /**
    * Create a new RemoteCacheManager using the supplied {@link Configuration}.
    * The RemoteCacheManager will be started automatically only if the start parameter is true
    *
    * @param configuration the configuration to use for this RemoteCacheManager
    * @param start         whether or not to start the manager on return from the constructor.
    * @since 5.3
    */
   public RemoteCacheManager(Configuration configuration, boolean start) {
      this.configuration = configuration;
      this.counterManager = new RemoteCounterManager();
      if (start) start();
   }

   /**
    * @since 5.3
    */
   @Override
   public Configuration getConfiguration() {
      return configuration;
   }

   /**
    * <p>Similar to {@link RemoteCacheManager#RemoteCacheManager(Configuration, boolean)}, but it will try to lookup
    * the config properties in the classpath, in a file named <tt>hotrod-client.properties</tt>. If no properties can
    * be found in the classpath, defaults will be used, attempting to connect to <tt>127.0.0.1:11222</tt></p>
    *
    * <p>Refer to
    * {@link ConfigurationBuilder} for a detailed list of available properties.</p>
    *
    * @param start whether or not to start the RemoteCacheManager
    * @throws HotRodClientException if such a file cannot be found in the classpath
    */
   public RemoteCacheManager(boolean start) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      builder.classLoader(cl);
      InputStream stream = FileLookupFactory.newInstance().lookupFile(HOTROD_CLIENT_PROPERTIES, cl);
      if (stream == null) {
         log.couldNotFindPropertiesFile(HOTROD_CLIENT_PROPERTIES);
      } else {
         try {
            builder.withProperties(loadFromStream(stream));
         } finally {
            Util.close(stream);
         }
      }
      this.configuration = builder.build();
      this.counterManager = new RemoteCounterManager();
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(boolean)} and it also starts the cache.
    */
   public RemoteCacheManager() {
      this(true);
   }

   /**
    * Retrieves a named cache from the remote server if the cache has been
    * defined, otherwise if the cache name is undefined, it will return null.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName or null if the cache
    * name has not been defined
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName) {
      return getCache(cacheName, configuration.forceReturnValues());
   }

   @Override
   public Set<String> getCacheNames() {
      OperationsFactory operationsFactory = new OperationsFactory(channelFactory, codec, configuration);
      String names = await(operationsFactory.newAdminOperation("@@cache@names", Collections.emptyMap()).execute());
      Set<String> cacheNames = new HashSet<>();
      // Simple pattern that matches the result which is represented as a JSON string array, e.g. ["cache1","cache2"]
      Pattern pattern = Pattern.compile(JSON_STRING_ARRAY_ELEMENT_REGEX);
      Matcher matcher = pattern.matcher(names);
      while (matcher.find()) {
         cacheNames.add(matcher.group(1));
      }
      return cacheNames;
   }

   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue) {
      return createRemoteCache(cacheName, forceReturnValue);
   }

   /**
    * Retrieves the default cache from the remote server.
    *
    * @return a remote cache instance that can be used to send requests to the
    * default cache in the server
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache() {
      return getCache(configuration.forceReturnValues());
   }

   @Override
   public <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue) {
      //As per the HotRod protocol specification, the default cache is identified by an empty string
      return createRemoteCache("", forceReturnValue);
   }

   public CompletableFuture<Void> startAsync() {
      // The default async executor service is dedicated for Netty, therefore here we'll use common FJP.
      return CompletableFuture.runAsync(start);
   }

   public CompletableFuture<Void> stopAsync() {
      return CompletableFuture.runAsync(stop);
   }

   @Override
   public void start() {
      channelFactory = new ChannelFactory();

      if (marshaller == null) {
         marshaller = configuration.marshaller();
         if (marshaller == null) {
            Class<? extends Marshaller> clazz = configuration.marshallerClass();
            if (clazz == GenericJBossMarshaller.class && !configuration.serialWhitelist().isEmpty())
               marshaller = new GenericJBossMarshaller(configuration.serialWhitelist());
            else
               marshaller = Util.getInstance(clazz);
         }
      }

      codec = CodecFactory.getCodec(configuration.version());

      listenerNotifier = new ClientListenerNotifier(codec, marshaller, channelFactory, configuration.serialWhitelist());
      ExecutorFactory executorFactory = configuration.asyncExecutorFactory().factory();
      if (executorFactory == null) {
         executorFactory = Util.getInstance(configuration.asyncExecutorFactory().factoryClass());
      }
      ExecutorService asyncExecutorService = executorFactory.getExecutor(configuration.asyncExecutorFactory().properties());
      channelFactory.start(codec, configuration, defaultCacheTopologyId, marshaller, asyncExecutorService,
            Collections.singletonList(listenerNotifier::failoverListeners));
      counterManager.start(channelFactory, codec, configuration, listenerNotifier);

      synchronized (cacheName2RemoteCache) {
         for (RemoteCacheHolder rcc : cacheName2RemoteCache.values()) {
            startRemoteCache(rcc);
         }
      }

      // Print version to help figure client version run
      log.version(RemoteCacheManager.class.getPackage().getImplementationVersion());

      warnAboutUberJarDuplicates();

      started = true;
   }

   private final void warnAboutUberJarDuplicates() {
      UberJarDuplicatedJarsWarner scanner = new ManifestUberJarDuplicatedJarsWarner();
      scanner.isClasspathCorrectAsync()
            .thenAcceptAsync(isClasspathCorrect -> {
               if (!isClasspathCorrect)
                  log.warnAboutUberJarDuplicates();
            });
   }

   /**
    * Stop the remote cache manager, disconnecting all existing connections.
    * As part of the disconnection, all registered client cache listeners will
    * be removed since client no longer can receive callbacks.
    */
   @Override
   public void stop() {
      if (isStarted()) {
         listenerNotifier.stop();
         counterManager.stop();
         channelFactory.destroy();
      }
      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public boolean switchToCluster(String clusterName) {
      return channelFactory.switchToCluster(clusterName);
   }

   @Override
   public boolean switchToDefaultCluster() {
      return channelFactory.switchToCluster(ChannelFactory.DEFAULT_CLUSTER_NAME);
   }

   private Properties loadFromStream(InputStream stream) {
      Properties properties = new Properties();
      try {
         properties.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties", e);
      }
      return properties;
   }

   @SuppressWarnings("unchecked")
   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName, Boolean forceReturnValueOverride) {
      synchronized (cacheName2RemoteCache) {
         RemoteCacheKey key = new RemoteCacheKey(cacheName, forceReturnValueOverride);
         if (!cacheName2RemoteCache.containsKey(key)) {
            RemoteCacheImpl<K, V> result = createRemoteCache(cacheName);
            RemoteCacheHolder rcc = new RemoteCacheHolder(result, forceReturnValueOverride);
            startRemoteCache(rcc);

            PingResult pingResult = result.resolveCompatibility();
            // If ping not successful assume that the cache does not exist
            // Default cache is always started, so don't do for it
            if (!cacheName.equals(RemoteCacheManager.DEFAULT_CACHE_NAME) &&
                  pingResult == PingResult.CACHE_DOES_NOT_EXIST) {
               return null;
            }

            result.start();
            // If ping on startup is disabled, or cache is defined in server
            cacheName2RemoteCache.put(key, rcc);
            return result;
         } else {
            return (RemoteCache<K, V>) cacheName2RemoteCache.get(key).remoteCache;
         }
      }
   }

   private <K, V> RemoteCacheImpl<K, V> createRemoteCache(String cacheName) {
      switch (configuration.nearCache().mode()) {
         case INVALIDATED:
            return new InvalidatedNearRemoteCache<>(this, cacheName,
                  createNearCacheService(configuration.nearCache()));
         case DISABLED:
         default:
            return new RemoteCacheImpl<>(this, cacheName);
      }
   }

   protected <K, V> NearCacheService<K, V> createNearCacheService(NearCacheConfiguration cfg) {
      return NearCacheService.create(cfg, listenerNotifier);
   }

   private void startRemoteCache(RemoteCacheHolder remoteCacheHolder) {
      RemoteCacheImpl<?, ?> remoteCache = remoteCacheHolder.remoteCache;
      OperationsFactory operationsFactory = new OperationsFactory(
            channelFactory, remoteCache.getName(), remoteCacheHolder.forceReturnValue, codec, listenerNotifier,
            configuration);
      remoteCache.init(marshaller, operationsFactory,
            configuration.keySizeEstimate(), configuration.valueSizeEstimate(), configuration.batchSize());
   }

   @Override
   public Marshaller getMarshaller() {
      return marshaller;
   }

   public static byte[] cacheNameBytes(String cacheName) {
      return cacheName.equals(DEFAULT_CACHE_NAME)
            ? HotRodConstants.DEFAULT_CACHE_NAME_BYTES
            : cacheName.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
   }

   public static byte[] cacheNameBytes() {
      return HotRodConstants.DEFAULT_CACHE_NAME_BYTES;
   }

   public RemoteCacheManagerAdmin administration() {
      OperationsFactory operationsFactory = new OperationsFactory(channelFactory, codec, configuration);
      return new RemoteCacheManagerAdminImpl(this, operationsFactory, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class),
            name -> {
               synchronized (cacheName2RemoteCache) {
                  // Remove any mappings
                  cacheName2RemoteCache.remove(new RemoteCacheKey(name, true));
                  cacheName2RemoteCache.remove(new RemoteCacheKey(name, false));
               }
            });
   }

   @Override
   public void close() throws IOException {
      stop();
   }

   CounterManager getCounterManager() {
      return counterManager;
   }

   /**
    * This method is not a part of the public API. It is exposed for internal purposes only.
    */
   public Codec getCodec() {
      return codec;
   }

   /**
    * This method is not a part of the public API. It is exposed for internal purposes only.
    */
   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   private static class RemoteCacheKey {

      final String cacheName;
      final boolean forceReturnValue;

      RemoteCacheKey(String cacheName, boolean forceReturnValue) {
         this.cacheName = cacheName;
         this.forceReturnValue = forceReturnValue;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof RemoteCacheKey)) return false;

         RemoteCacheKey that = (RemoteCacheKey) o;

         if (forceReturnValue != that.forceReturnValue) return false;
         return !(cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null);
      }

      @Override
      public int hashCode() {
         int result = cacheName != null ? cacheName.hashCode() : 0;
         result = 31 * result + (forceReturnValue ? 1 : 0);
         return result;
      }
   }

   private static class RemoteCacheHolder {
      final RemoteCacheImpl<?, ?> remoteCache;
      final boolean forceReturnValue;

      RemoteCacheHolder(RemoteCacheImpl<?, ?> remoteCache, boolean forceReturnValue) {
         this.remoteCache = remoteCache;
         this.forceReturnValue = forceReturnValue;
      }
   }
}
