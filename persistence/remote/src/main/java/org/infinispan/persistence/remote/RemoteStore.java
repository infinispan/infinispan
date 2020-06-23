package org.infinispan.persistence.remote;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec27;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.remote.configuration.AuthenticationConfiguration;
import org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.SslConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.persistence.remote.wrapper.HotRodEntryMarshaller;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Cache store that delegates the call to a infinispan cluster. Communication between this cache store and the remote
 * cluster is achieved through the java HotRod client: this assures fault tolerance and smart dispatching of calls to
 * the nodes that have the highest chance of containing the given key. This cache store supports both preloading
 * and <b>fetchPersistentState</b>.
 * <p/>
 * Purging elements is not possible, as HotRod does not support the fetching of all remote keys (this would be a
 * very costly operation as well). Purging takes place at the remote end (infinispan cluster).
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration
 * @see <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 * @since 4.1
 */
@ConfiguredBy(RemoteStoreConfiguration.class)
public class RemoteStore<K, V> implements NonBlockingStore<K, V> {

   private static final Log log = LogFactory.getLog(RemoteStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private RemoteStoreConfiguration configuration;

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   private InternalEntryFactory iceFactory;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";
   protected InitializationContext ctx;
   private MarshallableEntryFactory<K, V> entryFactory;
   private BlockingManager blockingManager;
   private KeyPartitioner keyPartitioner;
   private int segmentCount;

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.entryFactory = ctx.getMarshallableEntryFactory();
      this.blockingManager = ctx.getBlockingManager();
      this.keyPartitioner = ctx.getKeyPartitioner();

      ClusteringConfiguration clusterConfiguration = ctx.getCache().getCacheConfiguration().clustering();
      this.segmentCount = clusterConfiguration.hash().numSegments();

      final Marshaller marshaller;
      if (configuration.marshaller() != null) {
         marshaller = Util.getInstance(configuration.marshaller(), ctx.getCache().getAdvancedCache().getClassLoader());
      } else if (configuration.hotRodWrapping()) {
         marshaller = new HotRodEntryMarshaller(ctx.getByteBufferFactory());
      } else if (configuration.rawValues()) {
         ClassWhiteList whiteList = ctx.getCache().getCacheManager().getClassWhiteList();
         marshaller = new GenericJBossMarshaller(Thread.currentThread().getContextClassLoader(), whiteList);
      } else {
         marshaller = ctx.getPersistenceMarshaller();
      }

      // Segmented cannot work properly in a clustered cache without being shared
      if (clusterConfiguration.cacheMode().isClustered() && !configuration.shared() && configuration.segmented()) {
         throw log.segmentationRequiresBeingShared();
      }

      if (configuration.rawValues() && iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }

      // Make sure threads are marked as non blocking if user didn't specify
      configuration.properties().putIfAbsent("blocking", "false");

      ConfigurationBuilder builder = buildRemoteConfiguration(configuration, marshaller);

      return blockingManager.runBlocking(() -> {
         remoteCacheManager = new RemoteCacheManager(builder.build());

         if (configuration.remoteCacheName().isEmpty())
            remoteCache = remoteCacheManager.getCache();
         else
            remoteCache = remoteCacheManager.getCache(configuration.remoteCacheName());
      }, "RemoteStore-start");
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION,
            Characteristic.SHAREABLE);
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> remoteCacheManager.stop(), "RemoteStore-stop");
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return ((RemoteCacheImpl<?, ?>) remoteCache).ping()
            .handle((v, t) -> t == null && v.isSuccess());
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      if (configuration.rawValues()) {
         Object unwrappedKey;
         if (key instanceof WrappedByteArray) {
            unwrappedKey = ((WrappedByteArray) key).getBytes();
         } else {
            unwrappedKey = key;
         }
         CompletableFuture<MetadataValue<Object>> valueStage = remoteCache.getWithMetadataAsync(unwrappedKey);
         return valueStage.thenApply(metadataValue -> {
            if (metadataValue != null) {
               Metadata metadata = new EmbeddedMetadata.Builder()
                     .version(new NumericVersion(metadataValue.getVersion()))
                     .lifespan(metadataValue.getLifespan(), TimeUnit.SECONDS)
                     .maxIdle(metadataValue.getMaxIdle(), TimeUnit.SECONDS).build();
               long created = metadataValue.getCreated();
               long lastUsed = metadataValue.getLastUsed();
               Object realValue = metadataValue.getValue();
               if (realValue instanceof byte[]) {
                  realValue = new WrappedByteArray((byte[]) realValue);
               }
               return entryFactory.create(key, realValue, metadata, null, created, lastUsed);
            } else {
               return null;
            }
         });
      } else {
         Object unwrappedKey;
         if (key instanceof WrappedByteArray) {
            unwrappedKey = ((WrappedByteArray) key).getBytes();
         } else {
            unwrappedKey = key;
         }
         return remoteCache.getAsync(unwrappedKey)
               .thenApply(value -> value == null ? null : entryFactory.create(key, (MarshalledValue) value));
      }
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      if (key instanceof WrappedByteArray) {
         key = ((WrappedByteArray) key).getBytes();
      }
      return remoteCache.containsKeyAsync(key);
   }

   @Override
   public Flowable<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      Flowable<K> keyFlowable = Flowable.fromPublisher(remoteCache.publishEntries(Codec27.EMPTY_VAUE_CONVERTER,
            null, null, 512))
            .map(Map.Entry::getKey)
            .map(RemoteStore::wrap);

      if (filter != null) {
         keyFlowable = keyFlowable.filter(filter::test);
      }
      return keyFlowable;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      if (configuration.rawValues()) {
         Flowable<Map.Entry<Object, MetadataValue<Object>>> entryFlowable = Flowable.fromPublisher(remoteCache.publishEntriesWithMetadata(null, 512));
         if (filter != null) {
            entryFlowable = entryFlowable.filter(e -> filter.test(wrap(e.getKey())));
         }
         return entryFlowable.map(e -> {
            MetadataValue<Object> value = e.getValue();
            Metadata metadata = new EmbeddedMetadata.Builder()
                  .version(new NumericVersion(value.getVersion()))
                  .lifespan(value.getLifespan(), TimeUnit.SECONDS)
                  .maxIdle(value.getMaxIdle(), TimeUnit.SECONDS).build();
            long created = value.getCreated();
            long lastUsed = value.getLastUsed();
            Object realValue = value.getValue();
            return entryFactory.create(wrap(e.getKey()), wrap(realValue), metadata, null, created, lastUsed);
         });
      } else {
         Flowable<Map.Entry<Object, Object>> entryFlowable = Flowable.fromPublisher(
               remoteCache.publishEntries(null, null, null, 512));
         if (filter != null) {
            entryFlowable = entryFlowable.filter(e -> filter.test(wrap(e.getKey())));
         }
         // Technically we will send the metadata and value to the user, no matter what.
         return entryFlowable.map(e -> e.getValue() == null ? null : entryFactory.create(e.getKey(), (MarshalledValue) e.getValue()));
      }
   }

   private static <T> T wrap(Object obj) {
      if (obj instanceof byte[]) {
         obj = new WrappedByteArray((byte[]) obj);
      }
      return (T) obj;
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      if (segmentCount == segments.size()) {
         return remoteCache.sizeAsync();
      }
      return publishKeys(segments, null).count().toCompletionStage();
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      // Nothing to do here - expiration is controlled by the server
      return Flowable.empty();
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      if (trace) {
         log.tracef("Adding entry: %s", entry);
      }
      Metadata metadata = entry.getMetadata();
      long lifespan = metadata != null ? metadata.lifespan() : -1;
      long maxIdle = metadata != null ? metadata.maxIdle() : -1;
      Object key = getKey(entry);
      Object value = getValue(entry);

      return remoteCache.putAsync(key, value, toSeconds(lifespan, entry.getKey(), LIFESPAN), TimeUnit.SECONDS,
            toSeconds(maxIdle, entry.getKey(), MAXIDLE), TimeUnit.SECONDS)
            .thenApply(CompletableFutures.toNullFunction());
   }

   private Object getKey(MarshallableEntry entry) {
      Object key = entry.getKey();
      if (key instanceof WrappedByteArray)
         return ((WrappedByteArray) key).getBytes();
      return key;
   }

   private Object getValue(MarshallableEntry entry) {
      if (configuration.rawValues()) {
         Object value = entry.getValue();
         return value instanceof WrappedByteArray ? ((WrappedByteArray) value).getBytes() : value;
      }
      return entry.getMarshalledValue();
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      Completable removeCompletable = Flowable.fromPublisher(removePublisher)
            .flatMap(sp -> Flowable.fromPublisher(sp), publisherCount)
            .map(key -> key instanceof WrappedByteArray ? ((WrappedByteArray) key).getBytes() : key)
            .flatMapCompletable(key -> Completable.fromCompletionStage(remoteCache.removeAsync(key)), false, 10);

      Completable putCompletable = Flowable.fromPublisher(writePublisher)
            .flatMap(sp -> Flowable.fromPublisher(sp), publisherCount)
            .groupBy(MarshallableEntry::getMetadata)
            .flatMapCompletable(meFlowable -> meFlowable.buffer(configuration.maxBatchSize())
                  .flatMapCompletable(meList -> {
                     Map<Object, Object> map = meList.stream().collect(Collectors.toMap(this::getKey, this::getValue));

                     Metadata metadata = meFlowable.getKey();
                     long lifespan = metadata != null ? metadata.lifespan() : -1;
                     long maxIdle = metadata != null ? metadata.maxIdle() : -1;

                     return Completable.fromCompletionStage(remoteCache.putAllAsync(map, lifespan, TimeUnit.SECONDS,
                           maxIdle, TimeUnit.SECONDS));
                  }));
      return removeCompletable.mergeWith(putCompletable)
            .toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> clear() {
      return remoteCache.clearAsync();
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      if (key instanceof WrappedByteArray) {
         key = ((WrappedByteArray) key).getBytes();
      }
      // Less than ideal, but RemoteCache, since it extends Cache, can only
      // know whether the operation succeeded based on whether the previous
      // value is null or not.
      return remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).removeAsync(key)
            .thenApply(Objects::nonNull);
   }

   private long toSeconds(long millis, Object key, String desc) {
      if (millis > 0 && millis < 1000) {
         if (trace) {
            log.tracef("Adjusting %s time for key %s from %d millis to 1 sec, as milliseconds are not supported by HotRod",
                  desc, key, millis);
         }
         return 1;
      }
      return TimeUnit.MILLISECONDS.toSeconds(millis);
   }

   public void setInternalCacheEntryFactory(InternalEntryFactory iceFactory) {
      if (this.iceFactory != null) {
         throw new IllegalStateException();
      }
      this.iceFactory = iceFactory;
   }

   public RemoteCache<Object, Object> getRemoteCache() {
      return remoteCache;
   }

   private ConfigurationBuilder buildRemoteConfiguration(RemoteStoreConfiguration configuration, Marshaller marshaller) {
      ConfigurationBuilder builder = new ConfigurationBuilder();

      for (RemoteServerConfiguration s : configuration.servers()) {
         builder.addServer()
               .host(s.host())
               .port(s.port());
      }

      ConnectionPoolConfiguration poolConfiguration = configuration.connectionPool();
      Long connectionTimeout = configuration.connectionTimeout();
      Long socketTimeout = configuration.socketTimeout();

      builder.classLoader(configuration.getClass().getClassLoader())
            .balancingStrategy(configuration.balancingStrategy())
            .connectionPool()
            .exhaustedAction(ExhaustedAction.valueOf(poolConfiguration.exhaustedAction().toString()))
            .maxActive(poolConfiguration.maxActive())
            .minIdle(poolConfiguration.minIdle())
            .minEvictableIdleTime(poolConfiguration.minEvictableIdleTime())
            .connectionTimeout(connectionTimeout.intValue())
            .forceReturnValues(configuration.forceReturnValues())
            .keySizeEstimate(configuration.keySizeEstimate())
            .marshaller(marshaller)
            .asyncExecutorFactory().factoryClass(configuration.asyncExecutorFactory().factory().getClass())
            .asyncExecutorFactory().withExecutorProperties(configuration.asyncExecutorFactory().properties())
            .socketTimeout(socketTimeout.intValue())
            .tcpNoDelay(configuration.tcpNoDelay())
            .valueSizeEstimate(configuration.valueSizeEstimate())
            .version(configuration.protocol() == null ? ProtocolVersion.DEFAULT_PROTOCOL_VERSION : configuration.protocol());

      SslConfiguration ssl = configuration.security().ssl();
      if (ssl.enabled()) {
         builder.security().ssl()
               .enable()
               .keyStoreType(ssl.keyStoreType())
               .keyAlias(ssl.keyAlias())
               .keyStoreFileName(ssl.keyStoreFileName())
               .keyStorePassword(ssl.keyStorePassword())
               .keyStoreCertificatePassword(ssl.keyStoreCertificatePassword())
               .trustStoreFileName(ssl.trustStoreFileName())
               .trustStorePassword(ssl.trustStorePassword())
               .trustStoreType(ssl.trustStoreType())
               .protocol(ssl.protocol())
               .sniHostName(ssl.sniHostName());
      }
      AuthenticationConfiguration auth = configuration.security().authentication();
      if (auth.enabled()) {
         builder.security().authentication()
               .enable()
               .callbackHandler(auth.callbackHandler())
               .clientSubject(auth.clientSubject())
               .saslMechanism(auth.saslMechanism())
               .serverName(auth.serverName())
               .saslProperties(auth.saslProperties())
               .username(auth.username())
               .password(auth.password())
               .realm(auth.realm())
         ;
      }

      builder.withProperties(configuration.properties());
      return builder;
   }

   public RemoteStoreConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public boolean ignoreCommandWithFlags(long commandFlags) {
      return EnumUtil.containsAny(FlagBitSets.ROLLING_UPGRADE, commandFlags);
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      // Here for documentation purposes. This method should never be invoked as we only support segmented when shared
      return NonBlockingStore.super.addSegments(segments);
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      // Here for documentation purposes. This method should never be invoked as we only support segmented when shared
      return NonBlockingStore.super.removeSegments(segments);
   }
}
