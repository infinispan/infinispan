package org.infinispan.persistence.remote;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.remote.configuration.ConnectionPoolConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteServerConfiguration;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.persistence.remote.wrapper.HotRodEntryMarshaller;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

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
@ThreadSafe
public class RemoteStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(RemoteStore.class, Log.class);

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
         marshaller = Util.getInstance(configuration.marshaller(), ctx.getCache().getCacheConfiguration().classLoader());
      } else if (configuration.hotRodWrapping()) {
         marshaller = new HotRodEntryMarshaller(ctx.getByteBufferFactory());
      } else if (configuration.rawValues()) {
         marshaller = new GenericJBossMarshaller();
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
   public MarshalledEntry load(Object key) throws PersistenceException {
      if (configuration.rawValues()) {
         MetadataValue<?> value = remoteCache.getWithMetadata(key);
         if (value != null) {
            Metadata metadata = new EmbeddedMetadata.Builder()
                  .version(new NumericVersion(value.getVersion()))
                  .lifespan(value.getLifespan(), TimeUnit.SECONDS)
                  .maxIdle(value.getMaxIdle(), TimeUnit.SECONDS).build();
            long created = value.getCreated();
            long lastUsed = value.getLastUsed();
            return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, value.getValue(),
                                    new InternalMetadataImpl(metadata, created, lastUsed));
         } else {
            return null;
         }
      } else {
         return (MarshalledEntry) remoteCache.get(key);
      }
   }

   @Override
   public boolean contains(Object key) throws PersistenceException {
      return remoteCache.containsKey(key);
   }

   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      TaskContextImpl taskContext = new TaskContextImpl();
      for (Object key : remoteCache.keySet()) {
         if (taskContext.isStopped())
            break;
         if (filter == null || filter.shouldLoadKey(key)) {
            try {
               MarshalledEntry marshalledEntry = load(key);
               if (marshalledEntry != null) {
                  task.processEntry(marshalledEntry, taskContext);
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      }
   }

   @Override
   public int size() {
      return remoteCache.size();
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      //ignored
   }

   @Override
   public void write(MarshalledEntry entry) throws PersistenceException {
      if (log.isTraceEnabled()) {
         log.tracef("Adding entry: %s", entry);
      }
      InternalMetadata metadata = entry.getMetadata();
      long lifespan = metadata != null ? metadata.lifespan() : -1;
      long maxIdle = metadata != null ? metadata.maxIdle() : -1;
      remoteCache.put(entry.getKey(), configuration.rawValues() ? entry.getValue() : entry, toSeconds(lifespan, entry.getKey(), LIFESPAN), TimeUnit.SECONDS, toSeconds(maxIdle, entry.getKey(), MAXIDLE), TimeUnit.SECONDS);
   }

   @Override
   public void clear() throws PersistenceException {
      remoteCache.clear();
   }

   @Override
   public boolean delete(Object key) throws PersistenceException {
      // Less than ideal, but RemoteCache, since it extends Cache, can only
      // know whether the operation succeeded based on whether the previous
      // value is null or not.
      return remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(key) != null;
   }

   private long toSeconds(long millis, Object key, String desc) {
      if (millis > 0 && millis < 1000) {
         if (log.isTraceEnabled()) {
            log.tracef("Adjusting %s time for (k,v): (%s, %s) from %d millis to 1 sec, as milliseconds are not supported by HotRod",
                       desc ,key, millis);
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

      builder.balancingStrategy(configuration.balancingStrategy())
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
            .classLoader(configuration.getClass().getClassLoader())
            .pingOnStartup(configuration.pingOnStartup())
            .socketTimeout(socketTimeout.intValue())
            .tcpNoDelay(configuration.tcpNoDelay())
            .valueSizeEstimate(configuration.valueSizeEstimate());
      if (configuration.protocolVersion() != null)
         builder.protocolVersion(configuration.protocolVersion());
      else
         builder.protocolVersion(ConfigurationProperties.PROTOCOL_VERSION_12);
      if (configuration.transportFactory() != null)
         builder.transportFactory(configuration.transportFactory());

      return builder;
   }

   public RemoteStoreConfiguration getConfiguration() {
      return configuration;
   }
}
