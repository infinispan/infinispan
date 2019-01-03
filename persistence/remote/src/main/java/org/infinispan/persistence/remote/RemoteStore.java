package org.infinispan.persistence.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.persistence.Store;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.remote.configuration.AuthenticationConfiguration;
import org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.SslConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.persistence.remote.wrapper.HotRodEntryMarshaller;
import org.infinispan.persistence.spi.FlagAffectedStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;
import net.jcip.annotations.ThreadSafe;

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
@Store(shared = true)
@ThreadSafe
@ConfiguredBy(RemoteStoreConfiguration.class)
public class RemoteStore<K, V> implements SegmentedAdvancedLoadWriteStore<K, V>, FlagAffectedStore<K, V> {

   private static final Log log = LogFactory.getLog(RemoteStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private RemoteStoreConfiguration configuration;

   private volatile RemoteCacheManager remoteCacheManager;
   private volatile RemoteCache<Object, Object> remoteCache;

   private InternalEntryFactory iceFactory;
   private static final String LIFESPAN = "lifespan";
   private static final String MAXIDLE = "maxidle";
   protected InitializationContext ctx;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.configuration = ctx.getConfiguration();
   }

   @Override
   public void start() throws PersistenceException {
      final Marshaller marshaller;
      if (configuration.marshaller() != null) {
         marshaller = Util.getInstance(configuration.marshaller(), ctx.getCache().getAdvancedCache().getClassLoader());
      } else if (configuration.hotRodWrapping()) {
         marshaller = new HotRodEntryMarshaller(ctx.getByteBufferFactory());
      } else if (configuration.rawValues()) {
         ClassWhiteList whiteList = ctx.getCache().getCacheManager().getClassWhiteList();
         marshaller = new GenericJBossMarshaller(Thread.currentThread().getContextClassLoader(), whiteList);
      } else {
         marshaller = ctx.getMarshaller();
      }
      ConfigurationBuilder builder = buildRemoteConfiguration(configuration, marshaller);
      remoteCacheManager = new RemoteCacheManager(builder.build());

      if (configuration.remoteCacheName().equals(BasicCacheContainer.DEFAULT_CACHE_NAME))
         remoteCache = remoteCacheManager.getCache();
      else
         remoteCache = remoteCacheManager.getCache(configuration.remoteCacheName());
      if (configuration.rawValues() && iceFactory == null) {
         iceFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
      }
   }

   @Override
   public void stop() throws PersistenceException {
      remoteCacheManager.stop();
   }

   @Override
   public boolean isAvailable() {
      try {
         PingOperation.PingResponse pr = ((RemoteCacheImpl) remoteCache).ping();
         return pr.isSuccess();
      } catch (Exception e) {
         return false;
      }
   }

   @Override
   public MarshalledEntry<K, V> load(Object key) throws PersistenceException {
      if (configuration.rawValues()) {
         Object unwrappedKey;
         if (key instanceof WrappedByteArray) {
            unwrappedKey = ((WrappedByteArray) key).getBytes();
         } else {
            unwrappedKey = key;
         }
         MetadataValue<?> value = remoteCache.getWithMetadata(unwrappedKey);
         if (value != null) {
            Metadata metadata = new EmbeddedMetadata.Builder()
                  .version(new NumericVersion(value.getVersion()))
                  .lifespan(value.getLifespan(), TimeUnit.SECONDS)
                  .maxIdle(value.getMaxIdle(), TimeUnit.SECONDS).build();
            long created = value.getCreated();
            long lastUsed = value.getLastUsed();
            Object realValue = value.getValue();
            if (realValue instanceof byte[]) {
               realValue = new WrappedByteArray((byte[]) realValue);
            }
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, realValue,
                  new InternalMetadataImpl(metadata, created, lastUsed));
         } else {
            return null;
         }
      } else {
         if (key instanceof WrappedByteArray) {
            key = ((WrappedByteArray) key).getBytes();
         }
         return (MarshalledEntry<K, V>) remoteCache.get(key);
      }
   }

   @Override
   public boolean contains(Object key) throws PersistenceException {
      if (key instanceof WrappedByteArray) {
         key = ((WrappedByteArray) key).getBytes();
      }
      return remoteCache.containsKey(key);
   }

   @Override
   public Flowable<K> publishKeys(Predicate<? super K> filter) {
      return publishKeys(null, filter);
   }

   @Override
   public Flowable<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      Flowable<K> keyFlowable = entryFlowable(remoteCache.keySet(segments).iterator())
            .map(RemoteStore::wrap);
      if (filter != null) {
         keyFlowable = keyFlowable.filter(filter::test);
      }
      return keyFlowable;
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      return publishEntries(null, filter, fetchValue, fetchMetadata);
   }

   @Override
   public Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata) {
      if (!fetchValue && !fetchMetadata) {
         Flowable<K> keyFlowable = publishKeys(segments, filter);
         return keyFlowable.map(key -> ctx.getMarshalledEntryFactory().newMarshalledEntry(key, (Object) null, null));
      }
      if (configuration.rawValues()) {
         io.reactivex.functions.Predicate<Map.Entry<Object, ?>> filterToUse = filter == null ? null :
               e -> filter.test(wrap(e.getKey()));
         if (!fetchMetadata) {
            // Only pass segments if we are running segmented mode (denoted by keyPartitioner being non null)
            Flowable<Map.Entry<Object, Object>> entryFlowable = entryFlowable(remoteCache.entrySet(segments)
                  .iterator());
            if (filterToUse != null) {
               entryFlowable = entryFlowable.filter(filterToUse);
            }
            return entryFlowable.map(e -> ctx.getMarshalledEntryFactory().newMarshalledEntry(wrap(e.getKey()),
                  (V) wrap(e.getValue()), null));
         } else {
            Flowable<Map.Entry<Object, MetadataValue<Object>>> entryMetatdataFlowable = entryFlowable(
                  remoteCache.retrieveEntriesWithMetadata(segments, 512));
            if (filterToUse != null) {
               entryMetatdataFlowable = entryMetatdataFlowable.filter(filterToUse);
            }
            return entryMetatdataFlowable.map(e -> {
               MetadataValue<Object> value = e.getValue();
               Metadata metadata = new EmbeddedMetadata.Builder()
                     .version(new NumericVersion(value.getVersion()))
                     .lifespan(value.getLifespan(), TimeUnit.SECONDS)
                     .maxIdle(value.getMaxIdle(), TimeUnit.SECONDS).build();
               long created = value.getCreated();
               long lastUsed = value.getLastUsed();
               Object realValue = value.getValue();
               return ctx.getMarshalledEntryFactory().newMarshalledEntry(wrap(e.getKey()), wrap(realValue),
                     new InternalMetadataImpl(metadata, created, lastUsed));
            });
         }
      } else {
         Flowable<Map.Entry<Object, Object>> entryFlowable = entryFlowable(remoteCache.entrySet(segments).iterator());
         if (filter != null) {
            entryFlowable = entryFlowable.filter(e -> filter.test(wrap(e.getKey())));
         }
         // Technically we will send the metadata and value to the user, no matter what.
         return entryFlowable.map(e -> (MarshalledEntry<K, V>) e.getValue());
      }
   }

   private static <E> Flowable<E> entryFlowable(CloseableIterator<E> closeableIteratorSet) {
      return Flowable.using(
            Functions.justCallable(closeableIteratorSet),
            iter -> Flowable.fromIterable(() -> iter),
            AutoCloseable::close);
   }

   private static <T> T wrap(Object obj) {
      if (obj instanceof byte[]) {
         obj = new WrappedByteArray((byte[]) obj);
      }
      return (T) obj;
   }

   @Override
   public int size() {
      return remoteCache.size();
   }

   @Override
   public int size(IntSet segments) {
      return PersistenceUtil.count(this, segments);
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      //ignored
   }

   @Override
   public void write(MarshalledEntry entry) throws PersistenceException {
      if (trace) {
         log.tracef("Adding entry: %s", entry);
      }
      InternalMetadata metadata = entry.getMetadata();
      long lifespan = metadata != null ? metadata.lifespan() : -1;
      long maxIdle = metadata != null ? metadata.maxIdle() : -1;
      Object key = getKey(entry);
      Object value = getValue(entry);

      remoteCache.put(key, value, toSeconds(lifespan, entry.getKey(), LIFESPAN), TimeUnit.SECONDS,
            toSeconds(maxIdle, entry.getKey(), MAXIDLE), TimeUnit.SECONDS);
   }

   private Object getKey(MarshalledEntry entry) {
      Object key = entry.getKey();
      if (key instanceof WrappedByteArray)
         return ((WrappedByteArray) key).getBytes();
      return key;
   }

   private Object getValue(MarshalledEntry entry) {
      if (configuration.rawValues()) {
         Object value = entry.getValue();
         return value instanceof WrappedByteArray ? ((WrappedByteArray) value).getBytes() : value;
      }
      return entry;
   }

   @Override
   public void writeBatch(Iterable<MarshalledEntry<? extends K, ? extends V>> marshalledEntries) {
      Map<Object, Object> batch = new HashMap<>();
      for (MarshalledEntry entry : marshalledEntries) {
         batch.put(getKey(entry), getValue(entry));
         if (batch.size() == configuration.maxBatchSize()) {
            remoteCache.putAll(batch);
            batch.clear();
         }
      }

      if (!batch.isEmpty())
         remoteCache.putAll(batch);
   }

   @Override
   public void clear() throws PersistenceException {
      remoteCache.clear();
   }

   @Override
   public void clear(IntSet segments) {
      publishKeys(segments, null).blockingForEach(k -> remoteCache.remove(k));
   }

   @Override
   public boolean delete(Object key) throws PersistenceException {
      if (key instanceof WrappedByteArray) {
         key = ((WrappedByteArray) key).getBytes();
      }
      // Less than ideal, but RemoteCache, since it extends Cache, can only
      // know whether the operation succeeded based on whether the previous
      // value is null or not.
      return remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(key) != null;
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
            .maxIdle(poolConfiguration.maxIdle())
            .maxTotal(poolConfiguration.maxTotal())
            .minIdle(poolConfiguration.minIdle())
            .minEvictableIdleTime(poolConfiguration.minEvictableIdleTime())
            .testWhileIdle(poolConfiguration.testWhileIdle())
            .timeBetweenEvictionRuns(poolConfiguration.timeBetweenEvictionRuns())
            .connectionTimeout(connectionTimeout.intValue())
            .forceReturnValues(configuration.forceReturnValues())
            .keySizeEstimate(configuration.keySizeEstimate())
            .marshaller(marshaller)
            .asyncExecutorFactory().factoryClass(configuration.asyncExecutorFactory().factory().getClass())
            .socketTimeout(socketTimeout.intValue())
            .tcpNoDelay(configuration.tcpNoDelay())
            .valueSizeEstimate(configuration.valueSizeEstimate());
      if (configuration.protocolVersion() != null)
         builder.protocolVersion(configuration.protocolVersion());
      else
         builder.version(ProtocolVersion.DEFAULT_PROTOCOL_VERSION);
      if (configuration.transportFactory() != null)
         builder.transportFactory(configuration.transportFactory());
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
   public boolean shouldWrite(long commandFlags) {
      return !EnumUtil.containsAny(FlagBitSets.ROLLING_UPGRADE, commandFlags);
   }
}
