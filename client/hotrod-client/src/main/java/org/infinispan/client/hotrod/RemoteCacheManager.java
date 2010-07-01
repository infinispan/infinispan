package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodOperations;
import org.infinispan.client.hotrod.impl.protocol.HotRodOperationsImpl;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.SerializationMarshaller;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

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
 * <p>
 *  The cache manager is configured through a {@link java.util.Properties} object passed to the C-tor (there are also
 *  "simplified" C-tors that rely on default values). Bellow is the list of supported configuration elements:
 * <ul>
 *  <li>
 *  hotrod-servers - the initial list of hotrod servers to connect to, specified in the following format: host1:port1;host2:port2...
 *  At least one host:port must be specified.
 *  </li>
 *  <li>
 *  request-balancing-strategy - for replicated (vs distributed) hotrod server clusters, the client balances requests to the
 *  servers according to this strategy. Defaults to {@link org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy}
 *  </li>
 *  <li>
 * force-return-value - weather or not to implicitly {@link org.infinispan.client.hotrod.Flag#FORCE_RETURN_VALUE} for all calls.
 * Defaults to false.
 * </li>
 *  <li>
 * tcp-no-delay - TCP no delay flag switch. Defaults to true.
 * </li>
 * <br/>
 * <i>below is connection pooling config</i>:
 * <p/>
 *  <li>maxActive - controls the maximum number of connections per server that are allocated (checked out to client threads, or idle in
 * the pool) at one time. When non-positive, there is no limit to the number of connections per server. When maxActive
 * is reached, the connection pool for that server is said to be exhausted. The default setting for this parameter is
 * -1, i.e. there is no limit.</li>
 * <li>maxTotal - sets a global limit on the number persistent connections that can be in circulation within the combined set of
 * servers. When non-positive, there is no limit to the total number of persistent connections in circulation. When
 * maxTotal is exceeded, all connections pools are exhausted. The default setting for this parameter is -1 (no limit).
 * </li>
 *
 * <li>maxIdle - controls the maximum number of idle persistent connections, per server, at any time. When negative, there is no limit
 * to the number of connections that may be idle per server. The default setting for this parameter is -1.</li>
 *
 * <li>
 *   whenExhaustedAction - specifies what happens when asking for a connection from a server's pool, and that pool is exhausted. Possible values:
 *   <ul>
 *     <li> 0 - an exception will be thrown to the calling user</li>
 *     <li> 1 - the caller will block (invoke waits until a new or idle connections is available.
 *     <li> 2 - a new persistent connection will be created and and returned (essentially making maxActive meaningless.) </li>
 *   </ul>
 *   The default whenExhaustedAction setting is 1.
 * </li>
 *
 * <li>
 * Optionally, one may configure the pool to examine and possibly evict connections as they sit idle in the pool and to
 * ensure that a minimum number of idle connections is maintained for each server. This is performed by an "idle connection
 * eviction" thread, which runs asynchronously. The idle object evictor does not lock the pool
 * throughout its execution.  The idle connection eviction thread may be configured using the following attributes:
 * <ul>
 *  <li>timeBetweenEvictionRunsMillis - indicates how long the eviction thread should sleep before "runs" of examining idle
 *  connections. When non-positive, no eviction thread will be launched. The default setting for this parameter is
 *  2 minutes </li>
 *   <li> minEvictableIdleTimeMillis - specifies the minimum amount of time that an connection may sit idle in the pool before it
 *   is eligible for eviction due to idle time. When non-positive, no connection will be dropped from the pool due to
 *   idle time alone. This setting has no effect unless timeBetweenEvictionRunsMillis > 0. The default setting for this
 *   parameter is 1800000(30 minutes). </li>
 *   <li> testWhileIdle - indicates whether or not idle connections should be validated by sending an TCP packet to the server,
 *   during idle connection eviction runs.  Connections that fail to validate will be dropped from the pool. This setting
 *   has no effect unless timeBetweenEvictionRunsMillis > 0.  The default setting for this parameter is true.
 *   </li>
 *   <li>minIdle - sets a target value for the minimum number of idle connections (per server) that should always be available.
 *   If this parameter is set to a positive number and timeBetweenEvictionRunsMillis > 0, each time the idle connection
 *   eviction thread runs, it will try to create enough idle instances so that there will be minIdle idle instances
 *   available for each server.  The default setting for this parameter is 5 minutes. </li>
 * </ul>
 * </li>
 * <li>
 * async-executor-factory - the ExecutorFactory that will hold the thread pool used for async calls. It must implement
 * {@link org.infinispan.executors.ExecutorFactory}. If not specified, defaults to {@link DefaultAsyncExecutorFactory} </li>
 * <li>default-executor-factory.poolSize - used as a configuration for {@link DefaultAsyncExecutorFactory}, and defined the number
 * of threads to keep in the pool. If not specified defaults to 1. </li>
 * <li> default-executor-factory.queueSize - queue to use for holding async requests before they are executed. Defaults
 * to 100000</li>
 * <li>
 * consistent-hash.[version] - see {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory}
 * <ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheManager implements CacheContainer {

   private static Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";

   public static final String CONF_HOTROD_SERVERS = "hotrod-servers";

   public static final String OVERRIDE_HOTROD_SERVERS = "infinispan.hotrod-client.servers-default";


   private Properties props;
   private TransportFactory transportFactory;
   private HotRodMarshaller hotRodMarshaller;
   private boolean started = false;
   private boolean forceReturnValueDefault = false;
   private ExecutorService asyncExecutorService;
   private final Map<String, RemoteCacheImpl> cacheName2RemoteCache = new HashMap<String, RemoteCacheImpl>();
   private AtomicInteger topologyId = new AtomicInteger();


   /**
    * Builds a remote cache manager that relies on the provided {@link org.infinispan.client.hotrod.HotRodMarshaller} for marshalling
    * keys and values to be send over to the remote infinispan cluster.
    * @param hotRodMarshaller marshaller implementatin to be used
    * @param props other properties
    * @param start weather or not to start the manager on return from the constructor.
    */
   public RemoteCacheManager(HotRodMarshaller hotRodMarshaller, Properties props, boolean start) {
      this(props);
      this.hotRodMarshaller = hotRodMarshaller;
      if (log.isTraceEnabled()) {
         log.trace("Using explicitly set marshaller: " + hotRodMarshaller);
      }
      if (start) start();
   }

   /**
    * Same as {@link org.infinispan.client.hotrod.RemoteCacheManager#RemoteCacheManager(HotRodMarshaller, java.util.Properties, boolean)} with start = true.
    */
   public RemoteCacheManager(HotRodMarshaller hotRodMarshaller, Properties props) {
      this(hotRodMarshaller, props, false);
   }

   /**
    * Build a cache manager based on supplied properties.
    */
   public RemoteCacheManager(Properties props, boolean start) {
      this.props = props;
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
         props = new Properties();
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
      props = new Properties();
      props.put(TransportFactory.CONF_HOTROD_SERVERS, host + ":" + port);
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
      props = new Properties();
      props.put(TransportFactory.CONF_HOTROD_SERVERS, servers);
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
      String factory = props.getProperty("transport-factory");
      if (factory == null) {
         factory = TcpTransportFactory.class.getName();
         log.info("'transport-factory' factory not specified, using " + factory);
      }
      transportFactory = (TransportFactory) VHelper.newInstance(factory);
      String servers = props.getProperty(CONF_HOTROD_SERVERS);
      transportFactory.start(props, getStaticConfiguredServers(servers), topologyId);
      if (hotRodMarshaller == null) {
         String hotrodMarshallerClass = props.getProperty("marshaller");
         if (hotrodMarshallerClass == null) {
            hotrodMarshallerClass = SerializationMarshaller.class.getName();
            log.info("'marshaller' not specified, using " + hotrodMarshallerClass);
         }
         hotRodMarshaller = (HotRodMarshaller) VHelper.newInstance(hotrodMarshallerClass); 
      }

      String asyncExecutorClass = DefaultAsyncExecutorFactory.class.getName();
      if (props.contains("asyn-executor-factory")) {
         asyncExecutorClass = props.getProperty("asyn-executor-factory");
      }
      ExecutorFactory executorFactory = (ExecutorFactory) VHelper.newInstance(asyncExecutorClass);
      asyncExecutorService = executorFactory.getExecutor(props);


      if (props.get("force-return-value") != null && props.get("force-return-value").equals("true")) {
         forceReturnValueDefault = true;
      }
      synchronized (cacheName2RemoteCache) {
         for (RemoteCacheImpl remoteCache : cacheName2RemoteCache.values()) {
            startRemoteCache(remoteCache);
         }
      }
      started = true;
   }

   @Override
   public void stop() {
      transportFactory.destroy();
      started = false;
   }

   public boolean isStarted() {
      return started;
   }

   private void loadFromStream(InputStream stream) {
      props = new Properties();
      try {
         props.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties", e);
      }
   }

   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName, boolean forceReturnValue) {
      synchronized (cacheName2RemoteCache) {
         if (!cacheName2RemoteCache.containsKey(cacheName)) {
            RemoteCacheImpl<K, V> result = new RemoteCacheImpl<K, V>(this, cacheName, forceReturnValue);
            if (isStarted()) {
               startRemoteCache(result);
            }
            cacheName2RemoteCache.put(cacheName, result);
            return result;
         } else {
            return cacheName2RemoteCache.get(cacheName);
         }
      }
   }

   private <K, V> void startRemoteCache(RemoteCacheImpl<K, V> result) {
      HotRodOperations hotRodOperations = new HotRodOperationsImpl(result.getName(), transportFactory, topologyId);
      result.init(hotRodOperations, hotRodMarshaller, asyncExecutorService);
   }

   private Set<InetSocketAddress> getStaticConfiguredServers(String servers) {
      Set<InetSocketAddress> serverAddresses = new HashSet<InetSocketAddress>();
      if (servers == null) {
         servers = System.getProperty(OVERRIDE_HOTROD_SERVERS);
         if (servers != null) {
            log.info("Overwriting default server properties (-D" + OVERRIDE_HOTROD_SERVERS + ") with " + servers);
         } else {
            servers = "127.0.0.1:11311";
         }
         log.info("'hotrod-servers' property not specified in config, using " + servers);
      }
      StringTokenizer tokenizer = new StringTokenizer(servers, ";");
      while (tokenizer.hasMoreTokens()) {
         String server = tokenizer.nextToken();
         String[] serverDef = tokenizeServer(server);
         String serverHost = serverDef[0];
         int serverPort = Integer.parseInt(serverDef[1]);
         serverAddresses.add(new InetSocketAddress(serverHost, serverPort));
      }
      if (serverAddresses.isEmpty()) {
         throw new IllegalStateException("No hot-rod servers specified!");
      }
      return serverAddresses;
   }

   private String[] tokenizeServer(String server) {
      StringTokenizer t = new StringTokenizer(server, ":");
      return new String[]{t.nextToken(), t.nextToken()};
   }

}
