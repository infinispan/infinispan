package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.manager.CacheContainer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.util.Util.getInstance;

/**
 * Factory for {@link org.infinispan.client.hotrod.RemoteCache}s. <p/> <p> <b>Lifecycle:</b> </p> In order to be able to
 * use an {@link org.infinispan.client.hotrod.RemoteCache}, the {@link org.infinispan.client.hotrod.RemoteCacheManager}
 * must be started first: beside other things, this instantiates connections to Hotrod server(s). Starting the {@link
 * org.infinispan.client.hotrod.RemoteCacheManager} can be done either at creation by passing start==true to constructor
 * or by using a constructor that does that for you (see C-tor documentation); or after construction by calling {@link
 * #start()}.
 * <p/>
 * This is an "expensive" object, as it manages a set of persistent TCP connections to the hotrod servers. It is recommended
 * to only have one instance of this per JVM, and to cache it between calls to the server (i.e. remoteCache
 * operations).
 * <p/>
 * {@link #stop()} needs to be called explicitly in order to release all the resources (e.g. threads, TCP connections).
 * <p/>
 * <p/>
 * <b>Configuration:</b>
 * <p/>
 * The cache manager is configured through a {@link java.util.Properties} object passed to the constructor (there are also
 * "simplified" constructors that rely on default values).
 * <p/>
 * Below is the list of supported configuration elements:
 * <ul>
 * <li><tt>infinispan.client.hotrod.request_balancing_strategy</tt>, default = org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy.  For replicated (vs distributed) hotrod server clusters, the client balances requests to the
 * servers according to this strategy.</li>
 * <li><tt>infinispan.client.hotrod.server_list</tt>, default = 127.0.0.1:11311.  This is the initial list of hotrod servers to connect to, specified in the following format: host1:port1;host2:port2...
 * At least one host:port must be specified.</li>
 * <li><tt>infinispan.client.hotrod.force_return_values</tt>, default = false.  Whether or not to implicitly {@link org.infinispan.client.hotrod.Flag#FORCE_RETURN_VALUE} for all calls.</li>
 * <li><tt>infinispan.client.hotrod.tcp_no_delay</tt>, default = true.  Affects TCP NODELAY on the TCP stack.</li>
 * <li><tt>infinispan.client.hotrod.ping_on_startup</tt>, default = true.  If true, a ping request is sent to a back end server in order to fetch cluster's topology.</li>
 * <li><tt>infinispan.client.hotrod.transport_factory</tt>, default = org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory - controls which transport to use.  Currently only the TcpTransport is supported.</li>
 * <li><tt>infinispan.client.hotrod.marshaller</tt>, default = org.infinispan.marshall.jboss.GenericJBossMarshaller.  Allows you to specify a custom {@link org.infinispan.marshall.Marshaller} implementation to serialize and deserialize user objects.</li>
 * <li><tt>infinispan.client.hotrod.async_executor_factory</tt>, default = org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory.  Allows you to specify a custom asynchroous executor for async calls.</li>
 * <li><tt>infinispan.client.hotrod.default_executor_factory.pool_size</tt>, default = 10.  If the default executor is used, this configures the number of threads to initialize the executor with.</li>
 * <li><tt>infinispan.client.hotrod.default_executor_factory.queue_size</tt>, default = 100000.  If the default executor is used, this configures the queue size to initialize the executor with.</li>
 * <li><tt>infinispan.client.hotrod.hash_function_impl.1</tt>, default = org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1.  This specifies the version of the hash function and consistent hash algorithm in use, and is closely tied with the HotRod server version used.</li>
 * <li><tt>infinispan.client.hotrod.key_size_estimate</tt>, default = 64.  This hint allows sizing of byte buffers when serializing and deserializing keys, to minimize array resizing.</li>
 * <li><tt>infinispan.client.hotrod.value_size_estimate</tt>, default = 512.  This hint allows sizing of byte buffers when serializing and deserializing values, to minimize array resizing.</li>
 * </ul>
 * <br/>
 * <i>The following properties are related to connection pooling</i>:
 * <p/>
 * <ul>
 * <li><tt>maxActive</tt> - controls the maximum number of connections per server that are allocated (checked out to client threads, or idle in
 * the pool) at one time. When non-positive, there is no limit to the number of connections per server. When maxActive
 * is reached, the connection pool for that server is said to be exhausted. The default setting for this parameter is
 * -1, i.e. there is no limit.</li>
 * <li><tt>maxTotal</tt> - sets a global limit on the number persistent connections that can be in circulation within the combined set of
 * servers. When non-positive, there is no limit to the total number of persistent connections in circulation. When
 * maxTotal is exceeded, all connections pools are exhausted. The default setting for this parameter is -1 (no limit).
 * </li>
 * <p/>
 * <li><tt>maxIdle</tt> - controls the maximum number of idle persistent connections, per server, at any time. When negative, there is no limit
 * to the number of connections that may be idle per server. The default setting for this parameter is -1.</li>
 * <p/>
 * <li>
 * <tt>whenExhaustedAction</tt> - specifies what happens when asking for a connection from a server's pool, and that pool is exhausted. Possible values:
 * <ul>
 * <li> <tt>0</tt> - an exception will be thrown to the calling user</li>
 * <li> <tt>1</tt> - the caller will block (invoke waits until a new or idle connections is available.
 * <li> <tt>2</tt> - a new persistent connection will be created and and returned (essentially making maxActive meaningless.) </li>
 * </ul>
 * The default whenExhaustedAction setting is 1.
 * </li>
 * <p/>
 * <li>
 * Optionally, one may configure the pool to examine and possibly evict connections as they sit idle in the pool and to
 * ensure that a minimum number of idle connections is maintained for each server. This is performed by an "idle connection
 * eviction" thread, which runs asynchronously. The idle object evictor does not lock the pool
 * throughout its execution.  The idle connection eviction thread may be configured using the following attributes:
 * <ul>
 * <li><tt>timeBetweenEvictionRunsMillis</tt> - indicates how long the eviction thread should sleep before "runs" of examining idle
 * connections. When non-positive, no eviction thread will be launched. The default setting for this parameter is
 * 2 minutes </li>
 * <li> <tt>minEvictableIdleTimeMillis</tt> - specifies the minimum amount of time that an connection may sit idle in the pool before it
 * is eligible for eviction due to idle time. When non-positive, no connection will be dropped from the pool due to
 * idle time alone. This setting has no effect unless timeBetweenEvictionRunsMillis > 0. The default setting for this
 * parameter is 1800000(30 minutes). </li>
 * <li> <tt>testWhileIdle</tt> - indicates whether or not idle connections should be validated by sending an TCP packet to the server,
 * during idle connection eviction runs.  Connections that fail to validate will be dropped from the pool. This setting
 * has no effect unless timeBetweenEvictionRunsMillis > 0.  The default setting for this parameter is true.
 * </li>
 * <li><tt>minIdle</tt> - sets a target value for the minimum number of idle connections (per server) that should always be available.
 * If this parameter is set to a positive number and timeBetweenEvictionRunsMillis > 0, each time the idle connection
 * eviction thread runs, it will try to create enough idle instances so that there will be minIdle idle instances
 * available for each server.  The default setting for this parameter is 1. </li>
 * </ul>
 * </li>
 * <li>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheManager implements CacheContainer {

   private static Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";

   ConfigurationProperties config;
   private TransportFactory transportFactory;
   private Marshaller marshaller;
   private boolean started = false;
   private boolean forceReturnValueDefault = false;
   private ExecutorService asyncExecutorService;
   private final Map<String, RemoteCacheImpl> cacheName2RemoteCache = new HashMap<String, RemoteCacheImpl>();
   private AtomicInteger topologyId = new AtomicInteger();


   /**
    * Builds a remote cache manager that relies on the provided {@link Marshaller} for marshalling
    * keys and values to be send over to the remote Infinispan cluster.
    *
    * @param marshaller marshaller implementation to be used
    * @param props      other properties
    * @param start      weather or not to start the manager on return from the constructor.
    */
   public RemoteCacheManager(Marshaller marshaller, Properties props, boolean start) {
      this(props);
      setMarshaller(marshaller);
      if (log.isTraceEnabled())
         log.trace("Using explicitly set marshaller type: " + marshaller.getClass().getName());
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(Marshaller, java.util.Properties, boolean)} with start = true.
    */
   public RemoteCacheManager(Marshaller marshaller, Properties props) {
      this(marshaller, props, true);
   }

   /**
    * Build a cache manager based on supplied properties.
    */
   public RemoteCacheManager(Properties props, boolean start) {
      this.config = new ConfigurationProperties(props);
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties, boolean)}, and it also starts the cache (start==true).
    */
   public RemoteCacheManager(Properties props) {
      this(props, true);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in the
    * classpath, in a file named <tt>hotrod-client.properties</tt>. If no properties can be found in the classpath, the
    * server tries to connect to "127.0.0.1:11311" in start.
    *
    * @param start weather or not to start the RemoteCacheManager
    * @throws HotRodClientException if such a file cannot be found in the classpath
    */
   public RemoteCacheManager(boolean start) {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      InputStream stream = loader.getResourceAsStream(HOTROD_CLIENT_PROPERTIES);
      if (stream == null) {
         log.warn("Could not find '" + HOTROD_CLIENT_PROPERTIES + "' file in classpath, using defaults.");
         config = new ConfigurationProperties();
      } else {
         loadFromStream(stream);
      }
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(boolean)} and it also starts the cache.
    */
   public RemoteCacheManager() {
      this(true);
   }

   /**
    * Creates a remote cache manager aware of the hotrod server listening at host:port.
    *
    * @param start weather or not to start the RemoteCacheManager.
    */
   public RemoteCacheManager(String host, int port, boolean start) {
      config = new ConfigurationProperties(host + ":" + port);
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(String, int, boolean)} with start=true.
    */
   public RemoteCacheManager(String host, int port) {
      this(host, port, true);
   }

   /**
    * The given string should have the following structure: "host1:port2;host:port2...". Every host:port defines a
    * server.
    */
   public RemoteCacheManager(String servers, boolean start) {
      config = new ConfigurationProperties(servers);
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(String, boolean)}, with start=true.
    */
   public RemoteCacheManager(String servers) {
      this(servers, true);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in
    * supplied URL.
    *
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
    *
    * @param config
    */
   public RemoteCacheManager(URL config) {
      this(config, true);
   }

   public <K, V> RemoteCache<K, V> getCache(String cacheName) {
      return getCache(cacheName, forceReturnValueDefault);
   }

   public <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue) {
      return createRemoteCache(cacheName, forceReturnValue);
   }

   public <K, V> RemoteCache<K, V> getCache() {
      return getCache(forceReturnValueDefault);
   }

   public <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue) {
      //As per the HotRod protocol specification, the default cache is identified by an empty string
      return createRemoteCache("", forceReturnValue);
   }

   @Override
   public void start() {
      String factory = config.getTransportFactory();
      transportFactory = (TransportFactory) getInstance(factory);
      Collection<InetSocketAddress> servers = config.getServerList();
      transportFactory.start(config, servers, topologyId);
      if (marshaller == null) {
         String marshallerName = config.getMarshaller();
         setMarshaller((Marshaller) getInstance(marshallerName));
      }

      String asyncExecutorClass = config.getAsyncExecutorFactory();
      ExecutorFactory executorFactory = (ExecutorFactory) getInstance(asyncExecutorClass);
      asyncExecutorService = executorFactory.getExecutor(config.getProperties());

      forceReturnValueDefault = config.getForceReturnValues();

      synchronized (cacheName2RemoteCache) {
         for (RemoteCacheImpl remoteCache : cacheName2RemoteCache.values()) {
            startRemoteCache(remoteCache);
         }
      }
      started = true;
   }

   @Override
   public void stop() {
      if (isStarted()) {
         transportFactory.destroy();
      }
      started = false;
   }

   public boolean isStarted() {
      return started;
   }

   private void loadFromStream(InputStream stream) {
      Properties properties = new Properties();
      try {
         properties.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties", e);
      }
      config = new ConfigurationProperties(properties);
   }

   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName, boolean forceReturnValue) {
      synchronized (cacheName2RemoteCache) {
         if (!cacheName2RemoteCache.containsKey(cacheName)) {
            RemoteCacheImpl<K, V> result = new RemoteCacheImpl<K, V>(this, cacheName);
            startRemoteCache(result);
            cacheName2RemoteCache.put(cacheName, result);
            return result;
         } else {
            return cacheName2RemoteCache.get(cacheName);
         }
      }
   }

   private <K, V> void startRemoteCache(RemoteCacheImpl<K, V> result) {
      OperationsFactory operationsFactory = new OperationsFactory(transportFactory, result.getName(), topologyId, forceReturnValueDefault);
      result.init(marshaller, asyncExecutorService, operationsFactory, config.getKeySizeEstimate(), config.getValueSizeEstimate());
   }

   private void setMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
   }
}
