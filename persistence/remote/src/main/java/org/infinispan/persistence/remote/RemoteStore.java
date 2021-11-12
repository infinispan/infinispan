package org.infinispan.persistence.remote;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec27;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.remote.configuration.AuthenticationConfiguration;
import org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.SslConfiguration;
import org.infinispan.persistence.remote.logging.Log;
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

   private RemoteStoreConfiguration configuration;

   private RemoteCacheManager remoteCacheManager;
   private InternalRemoteCache<Object, Object> remoteCache;

   private InternalEntryFactory iceFactory;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";
   protected InitializationContext ctx;
   private MarshallableEntryFactory<K, V> entryFactory;
   private BlockingManager blockingManager;
   private int segmentCount;
   private boolean supportsSegmentation;

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
      this.entryFactory = ctx.getMarshallableEntryFactory();
      this.blockingManager = ctx.getBlockingManager();

      Configuration cacheConfiguration = ctx.getCache().getCacheConfiguration();
      ClusteringConfiguration clusterConfiguration = cacheConfiguration.clustering();
      this.segmentCount = clusterConfiguration.hash().numSegments();

      final Marshaller marshaller;
      if (configuration.marshaller() != null) {
         marshaller = Util.getInstance(configuration.marshaller(), ctx.getCache().getAdvancedCache().getClassLoader());
      } else {
         // If rawValues are required, then it's necessary to utilise the user marshaller directly to prevent objects being wrapped with a MarshallableUserObject
         marshaller = configuration.rawValues() ? ctx.getPersistenceMarshaller().getUserMarshaller() : ctx.getPersistenceMarshaller();
      }

      if (clusterConfiguration.cacheMode().isClustered() && !configuration.shared()) {
         throw log.clusteredRequiresBeingShared();
      }

      if (configuration.rawValues() && iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }

      ConfigurationBuilder builder = buildRemoteConfiguration(configuration, marshaller);

      return blockingManager.supplyBlocking(() -> {
         remoteCacheManager = new RemoteCacheManager(builder.build());

         if (configuration.remoteCacheName().isEmpty())
            remoteCache = (InternalRemoteCache<Object, Object>) remoteCacheManager.getCache();
         else
            remoteCache = (InternalRemoteCache<Object, Object>) remoteCacheManager.getCache(configuration.remoteCacheName());

         return remoteCache.ping();
      }, "RemoteStore-start")
            .thenCompose(Function.identity())
            .thenAccept(pingResponse -> {
               String cacheName = ctx.getCache().getName();

               MediaType serverKeyStorageType = pingResponse.getKeyMediaType();
               MediaType serverValueStorageType = pingResponse.getValueMediaType();

               DataFormat.Builder dataFormatBuilder = DataFormat.builder().from(remoteCache.getDataFormat());
               Integer numSegments = remoteCache.getCacheTopologyInfo().getNumSegments();
               boolean segmentsMatch;
               if (numSegments == null) {
                  log.debugf("Remote Store for cache %s cannot support segmentation as the number of segments was not found from the remote cache", cacheName);
                  segmentsMatch = false;
               } else {
                  segmentsMatch = numSegments == segmentCount;
                  if (segmentsMatch) {
                     log.debugf("Remote Store for cache %s can support segmentation as the number of segments matched the remote cache", cacheName);
                  } else {
                     log.debugf("Remote Store for cache %s cannot support segmentation as the number of segments %d do not match the remote cache %d",
                           cacheName, segmentCount, numSegments);
                  }
               }
               if (!segmentsMatch && configuration.segmented()) {
                  throw log.segmentationRequiresEqualSegments(segmentCount, numSegments);
               }
               StorageConfigurationManager storageConfigurationManager = ctx.getCache().getAdvancedCache().getComponentRegistry()
                     .getComponent(StorageConfigurationManager.class);

               MediaType localKeyStorageType = storageConfigurationManager.getKeyStorageMediaType();
               // When it isn't raw values we store as a Marshalled entry, so we have object storage for the value
               MediaType localValueStorageType = configuration.rawValues() ?
                     storageConfigurationManager.getValueStorageMediaType() : MediaType.APPLICATION_OBJECT;

               // Older servers don't provide media type information
               if ((serverKeyStorageType == null || serverKeyStorageType.match(MediaType.APPLICATION_UNKNOWN))
                     && localKeyStorageType.isBinary()) {
                  dataFormatBuilder.keyMarshaller(IdentityMarshaller.INSTANCE);
               }
               if ((serverValueStorageType == null || serverValueStorageType.match(MediaType.APPLICATION_UNKNOWN))
                     && localValueStorageType.isBinary()) {
                  dataFormatBuilder.valueMarshaller(IdentityMarshaller.INSTANCE);
               }
               supportsSegmentation = localKeyStorageType.equals(serverKeyStorageType);
               if (supportsSegmentation) {
                  dataFormatBuilder.keyType(localKeyStorageType.isBinary() ? localKeyStorageType : marshaller.mediaType());
                  dataFormatBuilder.keyMarshaller(localKeyStorageType.isBinary() ? IdentityMarshaller.INSTANCE : marshaller);
               } else if (configuration.segmented()) {
                  throw log.segmentationRequiresEqualMediaTypes(localKeyStorageType, serverKeyStorageType);
               }
               if (localValueStorageType.equals(serverValueStorageType)) {
                  dataFormatBuilder.valueType(localValueStorageType.isBinary() ? localValueStorageType : marshaller.mediaType());
                  dataFormatBuilder.valueMarshaller(localValueStorageType.isBinary() ? IdentityMarshaller.INSTANCE : marshaller);
               }
               DataFormat dataFormat = dataFormatBuilder.build();
               if (log.isTraceEnabled()) {
                  log.tracef("Data format for RemoteStore on cache %s is %s", cacheName, dataFormat);
               }
               remoteCache = remoteCache.withDataFormat(dataFormat);
            });
   }

   @Override
   public Set<Characteristic> characteristics() {
      Set<Characteristic> characteristics = EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION,
            Characteristic.SHAREABLE);
      if (supportsSegmentation) {
         characteristics.add(Characteristic.SEGMENTABLE);
      }
      return characteristics;
   }

   @Override
   public CompletionStage<Void> stop() {
      return blockingManager.runBlocking(() -> remoteCacheManager.stop(), "RemoteStore-stop");
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      if (remoteCache != null) {
         return remoteCache.ping()
               .handle((v, t) -> t == null && v.isSuccess());
      } else {
         return CompletableFutures.completedFalse();
      }
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      if (configuration.rawValues()) {
         Object unwrappedKey = unwrap(key);
         return remoteCache.getWithMetadataAsync(unwrappedKey).thenApply(metadataValue -> {
            if (metadataValue == null) {
               return null;
            }
            Metadata metadata = new EmbeddedMetadata.Builder()
                  .version(new NumericVersion(metadataValue.getVersion()))
                  .lifespan(metadataValue.getLifespan(), TimeUnit.SECONDS)
                  .maxIdle(metadataValue.getMaxIdle(), TimeUnit.SECONDS).build();
            long created = metadataValue.getCreated();
            long lastUsed = metadataValue.getLastUsed();
            Object realValue = wrap(metadataValue.getValue());
            return entryFactory.create(key, realValue, metadata, null, created, lastUsed);
         });
      } else {
         Object unwrappedKey = unwrap(key);
         return remoteCache.getAsync(unwrappedKey)
               .thenApply(value -> {
                  if(value == null) {
                     return null;
                  }
                  if(value instanceof MarshalledValue) {
                     return entryFactory.create(key, (MarshalledValue) value);
                  }
                  return entryFactory.create(key, value);
               });
      }
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      key = unwrap(key);
      return remoteCache.containsKeyAsync(key);
   }

   @Override
   public Flowable<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      // We assume our segments don't map to the remote node when segmentation is disabled
      IntSet segmentsToUse = configuration.segmented() ? segments : null;
      Flowable<K> keyFlowable = Flowable.fromPublisher(remoteCache.publishEntries(Codec27.EMPTY_VALUE_CONVERTER,
            null, segmentsToUse, 512))
            .map(Map.Entry::getKey)
            .map(RemoteStore::wrap);

      if (filter != null) {
         keyFlowable = keyFlowable.filter(filter::test);
      }
      return keyFlowable;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      // We assume our segments don't map to the remote node when segmentation is disabled
      IntSet segmentsToUse = configuration.segmented() ? segments : null;
      if (configuration.rawValues()) {
         Flowable<Map.Entry<Object, MetadataValue<Object>>> entryFlowable = Flowable.fromPublisher(remoteCache.publishEntriesWithMetadata(segmentsToUse, 512));
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
               remoteCache.publishEntries(null, null, segmentsToUse, 512));
         if (filter != null) {
            entryFlowable = entryFlowable.filter(e -> filter.test(wrap(e.getKey())));
         }
         // Technically we will send the metadata and value to the user, no matter what.
         return entryFlowable.map(e -> e.getValue() == null ? null : entryFactory.create(wrap(e.getKey()), (MarshalledValue) e.getValue()));
      }
   }

   private static <T> T wrap(Object obj) {
      if (obj instanceof byte[]) {
         obj = new WrappedByteArray((byte[]) obj);
      }
      return (T) obj;
   }

   private static <T> T unwrap(Object obj) {
      if (obj instanceof WrappedByteArray) {
         return (T) ((WrappedByteArray) obj).getBytes();
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
      if (log.isTraceEnabled()) {
         log.tracef("Adding entry: %s", entry);
      }
      Metadata metadata = entry.getMetadata();
      long lifespan = metadata != null ? toSeconds(metadata.lifespan(), entry.getKey(), LIFESPAN) : -1;
      long maxIdle = metadata != null ? toSeconds(metadata.maxIdle(), entry.getKey(), MAXIDLE) : -1;
      Object key = getKey(entry);
      Object value = getValue(entry);

      return remoteCache.putAsync(key, value, lifespan, TimeUnit.SECONDS, maxIdle, TimeUnit.SECONDS)
            .thenApply(CompletableFutures.toNullFunction());
   }

   private Object getKey(MarshallableEntry entry) {
      return unwrap(entry.getKey());
   }

   private Object getValue(MarshallableEntry entry) {
      if (configuration.rawValues()) {
         return unwrap(entry.getValue());
      }
      return entry.getMarshalledValue();
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      Completable removeCompletable = Flowable.fromPublisher(removePublisher)
            .flatMap(Flowable::fromPublisher, publisherCount)
            .map(RemoteStore::unwrap)
            .flatMapCompletable(key -> Completable.fromCompletionStage(remoteCache.removeAsync(key)), false, 10);

      Completable putCompletable = Flowable.fromPublisher(writePublisher)
            .flatMap(Flowable::fromPublisher, publisherCount)
            .groupBy(MarshallableEntry::getMetadata)
            .flatMapCompletable(meFlowable -> meFlowable.buffer(configuration.maxBatchSize())
                  .flatMapCompletable(meList -> {
                     Map<Object, Object> map = meList.stream().collect(Collectors.toMap(this::getKey, this::getValue));

                     Metadata metadata = meFlowable.getKey();
                     long lifespan = metadata != null ? toSeconds(metadata.lifespan(), "batch", LIFESPAN) : -1;
                     long maxIdle = metadata != null ? toSeconds(metadata.maxIdle(), "batch", MAXIDLE) : -1;

                     return Completable.fromCompletionStage(remoteCache.putAllAsync(map, lifespan, TimeUnit.SECONDS,
                           maxIdle, TimeUnit.SECONDS));
                  }));
      return removeCompletable.mergeWith(putCompletable)
            .toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> clear() {
      if (remoteCache != null) {
         return remoteCache.clearAsync();
      } else {
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      key = unwrap(key);
      return remoteCache.removeAsync(key)
            .thenApply(v -> null);
   }

   private long toSeconds(long millis, Object key, String desc) {
      if (millis > 0 && millis < 1000) {
         if (log.isTraceEnabled()) {
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

      ConfigurationBuilder builder = (configuration.uri() != null
            && !configuration.uri().isEmpty())
                  ? HotRodURI.create(configuration.uri()).toConfigurationBuilder()
                  : new ConfigurationBuilder();

      List<RemoteServerConfiguration> servers = configuration.servers();
      for (RemoteServerConfiguration s : servers) {
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

      Properties propertiesToUse;
      Properties actualProperties = configuration.properties();
      if (!actualProperties.contains("blocking")) {
         // Need to make a copy to not change the actual configuration properties
         propertiesToUse = new Properties(actualProperties);
         // Make sure threads are marked as non blocking if user didn't specify
         propertiesToUse.put("blocking", "false");
      } else {
         propertiesToUse = actualProperties;
      }

      builder.withProperties(propertiesToUse);
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
