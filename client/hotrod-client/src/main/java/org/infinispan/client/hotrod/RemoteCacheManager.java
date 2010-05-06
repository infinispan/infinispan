package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.client.hotrod.impl.HotrodOperations;
import org.infinispan.client.hotrod.impl.HotrodOperationsImpl;
import org.infinispan.client.hotrod.impl.HotrodMarshaller;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.SerializationMarshaller;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class RemoteCacheManager implements CacheContainer, Lifecycle {

   private static Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";

   public static final String CONF_HOTROD_SERVERS = "hotrod-servers";

   public static final String OVERRIDE_HOTROD_SERVERS = "infinispan.hotrod-client.servers-default";


   private Properties props;
   private TransportFactory transportFactory;
   private String hotrodMarshaller;
   private boolean started = false;
   private boolean forceReturnValueDefault = false;
   private ExecutorService asyncExecutorService;


   /**
    * Build a cache manager based on supplied given properties. TODO - add a list of all possible configuration
    * parameters here
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
    * classpath, in a file named <tt>hotrod-client.properties</tt>.
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
    * @param start weather or not to start the RemoteCacheManager.
    */
   public RemoteCacheManager(String host, int port, boolean start) {
      props = new Properties();
      props.put(TransportFactory.CONF_HOTROD_SERVERS, host + ":" + port);
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(String, int)} with start=true.
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
      return this.getCache(cacheName, forceReturnValueDefault);
   }

   public <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue) {
      return createRemoteCache(cacheName, forceReturnValue);
   }

   public <K, V> RemoteCache<K, V> getCache() {
      return this.getCache(forceReturnValueDefault);
   }
   public <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue) {
      return createRemoteCache(DefaultCacheManager.DEFAULT_CACHE_NAME, forceReturnValue);
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
      transportFactory.start(props, getStaticConfiguredServers(servers));
      hotrodMarshaller = props.getProperty("marshaller");

      String asyncExecutorClass = DefaultAsyncExecutorFactory.class.getName();
      if (props.contains("asyn-executor-factory")) {
         asyncExecutorClass = props.getProperty("asyn-executor-factory");
      }
      ExecutorFactory executorFactory = (ExecutorFactory) VHelper.newInstance(asyncExecutorClass);
      asyncExecutorService = executorFactory.getExecutor(props);
      

      if (hotrodMarshaller == null) {
         hotrodMarshaller = SerializationMarshaller.class.getName();
         log.info("'marshaller' not specified, using " + hotrodMarshaller);
      }
      if (props.get("force-return-value") != null && props.get("force-return-value").equals("true")) {
          forceReturnValueDefault = true;
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
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties",e);
      }
   }

   private <K, V> RemoteCache<K, V> createRemoteCache(String cacheName, boolean forceReturnValue) {
      HotrodMarshaller marshaller = (HotrodMarshaller) VHelper.newInstance(hotrodMarshaller);
      HotrodOperations hotrodOperations = new HotrodOperationsImpl(cacheName, transportFactory);
      return new RemoteCacheImpl<K, V>(hotrodOperations, marshaller, cacheName, this, asyncExecutorService, forceReturnValue);
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
