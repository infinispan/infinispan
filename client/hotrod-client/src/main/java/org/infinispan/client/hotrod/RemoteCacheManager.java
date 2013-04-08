/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ServerConfiguration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation.PingResult;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.CodecFactory;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.SysPropertyActions;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

/**
 * Factory for {@link org.infinispan.client.hotrod.RemoteCache}s. <p/> <p> <b>Lifecycle:</b> </p> In order to be able to
 * use an {@link org.infinispan.client.hotrod.RemoteCache}, the {@link org.infinispan.client.hotrod.RemoteCacheManager}
 * must be started first: beside other things, this instantiates connections to Hot Rod server(s). Starting the {@link
 * org.infinispan.client.hotrod.RemoteCacheManager} can be done either at creation by passing start==true to constructor
 * or by using a constructor that does that for you (see C-tor documentation); or after construction by calling {@link
 * #start()}.
 * <p/>
 * This is an "expensive" object, as it manages a set of persistent TCP connections to the Hot Rod servers. It is recommended
 * to only have one instance of this per JVM, and to cache it between calls to the server (i.e. remoteCache
 * operations).
 * <p/>
 * {@link #stop()} needs to be called explicitly in order to release all the resources (e.g. threads, TCP connections).
 * <p/>
 * <p/>
 * <b>Configuration:</b>
 * <p/>
 * The cache manager is configured through a {@link Configuration} object passed to the constructor (there are also
 * "simplified" constructors that rely on default values). If migrating from a previous version of Infinispan, where {@link Properties}
 * were used to configure the RemoteCacheManager, please use the {@link ConfigurationBuilder#withProperties(Properties)} method.
 * <p/>
 * Below is the list of supported configuration elements:
 * <ul>
 * <li><tt>infinispan.client.hotrod.request_balancing_strategy</tt>, default = org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy.  For replicated (vs distributed) Hot Rod server clusters, the client balances requests to the
 * servers according to this strategy.</li>
 * <li><tt>infinispan.client.hotrod.server_list</tt>, default = 127.0.0.1:11222.  This is the initial list of Hot Rod servers to connect to, specified in the following format: host1:port1;host2:port2...
 * At least one host:port must be specified.</li>
 * <li><tt>infinispan.client.hotrod.force_return_values</tt>, default = false.  Whether or not to implicitly {@link org.infinispan.client.hotrod.Flag#FORCE_RETURN_VALUE} for all calls.</li>
 * <li><tt>infinispan.client.hotrod.tcp_no_delay</tt>, default = true.  Affects TCP NODELAY on the TCP stack.</li>
 * <li><tt>infinispan.client.hotrod.ping_on_startup</tt>, default = true.  If true, a ping request is sent to a back end server in order to fetch cluster's topology.</li>
 * <li><tt>infinispan.client.hotrod.transport_factory</tt>, default = org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory - controls which transport to use.  Currently only the TcpTransport is supported.</li>
 * <li><tt>infinispan.client.hotrod.marshaller</tt>, default = org.infinispan.marshall.jboss.GenericJBossMarshaller.  Allows you to specify a custom {@link org.infinispan.marshall.Marshaller} implementation to serialize and deserialize user objects. For portable serialization payloads, you should configure the marshaller to be {@link org.infinispan.client.hotrod.marshall.ApacheAvroMarshaller}</li>
 * <li><tt>infinispan.client.hotrod.async_executor_factory</tt>, default = org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory.  Allows you to specify a custom asynchroous executor for async calls.</li>
 * <li><tt>infinispan.client.hotrod.default_executor_factory.pool_size</tt>, default = 10.  If the default executor is used, this configures the number of threads to initialize the executor with.</li>
 * <li><tt>infinispan.client.hotrod.default_executor_factory.queue_size</tt>, default = 100000.  If the default executor is used, this configures the queue size to initialize the executor with.</li>
 * <li><tt>infinispan.client.hotrod.hash_function_impl.1</tt>, default = It uses the hash function specified by the server in the responses as indicated in {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory}.  This specifies the version of the hash function and consistent hash algorithm in use, and is closely tied with the HotRod server version used.</li>
 * <li><tt>infinispan.client.hotrod.key_size_estimate</tt>, default = 64.  This hint allows sizing of byte buffers when serializing and deserializing keys, to minimize array resizing.</li>
 * <li><tt>infinispan.client.hotrod.value_size_estimate</tt>, default = 512.  This hint allows sizing of byte buffers when serializing and deserializing values, to minimize array resizing.</li>
 * <li><tt>infinispan.client.hotrod.socket_timeout</tt>, default = 60000 (60 seconds).  This property defines the maximum socket read timeout before giving up waiting for bytes from the server.</li>
 * <li><tt>infinispan.client.hotrod.protocol_version</tt>, default = 1.1 .This property defines the protocol version that this client should use. Other valid values include 1.0.</li>
 * <li><tt>infinispan.client.hotrod.connect_timeout</tt>, default = 60000 (60 seconds).  This property defines the maximum socket connect timeout before giving up connecting to the server.</li>
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
 * <li> <tt>2</tt> - a new persistent connection will be created and returned (essentially making maxActive meaningless.) </li>
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
public class RemoteCacheManager implements BasicCacheContainer {

   private static final Log log = LogFactory.getLog(RemoteCacheManager.class);

   public static final String HOTROD_CLIENT_PROPERTIES = "hotrod-client.properties";
   private volatile boolean started = false;
   private final Map<String, RemoteCacheHolder> cacheName2RemoteCache = new HashMap<String, RemoteCacheHolder>();
   // Use an invalid topologyID (-1) so we always get a topology update on connection.
   private AtomicInteger topologyId = new AtomicInteger(-1);
   private Configuration configuration;
   private Codec codec;

   private Marshaller marshaller;
   private TransportFactory transportFactory;
   private ExecutorService asyncExecutorService;

   /**
    *
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
    *
    * Create a new RemoteCacheManager using the supplied {@link Configuration}.
    * The RemoteCacheManager will be started automatically only if the start parameter is true
    *
    * @param configuration the configuration to use for this RemoteCacheManager
    * @param start whether or not to start the manager on return from the constructor.
    * @since 5.3
    */
   public RemoteCacheManager(Configuration configuration, boolean start) {
      this.configuration = configuration;
      if (start) start();
   }

   /**
    * Builds a remote cache manager that relies on the provided {@link Marshaller} for marshalling
    * keys and values to be send over to the remote Infinispan cluster.
    *
    * @param marshaller marshaller implementation to be used
    * @param props      other properties
    * @param start      whether or not to start the manager on return from the constructor.
    */
   @Deprecated
   public RemoteCacheManager(Marshaller marshaller, Properties props, boolean start) {
      this(new ConfigurationBuilder().classLoader(Thread.currentThread().getContextClassLoader()).withProperties(props).marshaller(marshaller.getClass()).build(), start);
   }

   /**
    * Builds a remote cache manager that relies on the provided {@link Marshaller} for marshalling
    * keys and values to be send over to the remote Infinispan cluster.
    *
    * @param marshaller marshaller implementation to be used
    * @param props      other properties
    * @param start      weather or not to start the manager on return from the constructor.
    */
   @Deprecated
   public RemoteCacheManager(Marshaller marshaller, Properties props, boolean start, ClassLoader classLoader, ExecutorFactory asyncExecutorFactory) {
      this(new ConfigurationBuilder().classLoader(classLoader).withProperties(props).marshaller(marshaller).asyncExecutorFactory().factory(asyncExecutorFactory).build(), start);
   }

   /**
    * Same as {@link #RemoteCacheManager(Marshaller, java.util.Properties, boolean)} with start = true.
    */
   @Deprecated
   public RemoteCacheManager(Marshaller marshaller, Properties props) {
      this(marshaller, props, true);
   }

   /**
    * Same as {@link #RemoteCacheManager(Marshaller, java.util.Properties, boolean)} with start = true.
    */
   @Deprecated
   public RemoteCacheManager(Marshaller marshaller, Properties props, ExecutorFactory asyncExecutorFactory) {
      this(new ConfigurationBuilder().withProperties(props).marshaller(marshaller).asyncExecutorFactory().factory(asyncExecutorFactory).build());
   }

   /**
    * Same as {@link #RemoteCacheManager(Marshaller, java.util.Properties, boolean)} with start = true.
    */
   @Deprecated
   public RemoteCacheManager(Marshaller marshaller, Properties props, ClassLoader classLoader) {
      this(new ConfigurationBuilder().classLoader(classLoader).marshaller(marshaller).withProperties(props).build());
   }

   /**
    * Build a cache manager based on supplied properties.
    */
   @Deprecated
   public RemoteCacheManager(Properties props, boolean start) {
      this(new ConfigurationBuilder().withProperties(props).build(), start);
   }

   /**
    * Build a cache manager based on supplied properties.
    */
   @Deprecated
   public RemoteCacheManager(Properties props, boolean start, ClassLoader classLoader, ExecutorFactory asyncExecutorFactory) {
      this(new ConfigurationBuilder().classLoader(classLoader).asyncExecutorFactory().factory(asyncExecutorFactory).withProperties(props).build(), start);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties, boolean)}, and it also starts the cache (start==true).
    */
   @Deprecated
   public RemoteCacheManager(Properties props) {
      this(new ConfigurationBuilder().withProperties(props).build());
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties, boolean)}, and it also starts the cache (start==true).
    */
   @Deprecated
   public RemoteCacheManager(Properties props, ClassLoader classLoader) {
      this(new ConfigurationBuilder().classLoader(classLoader).withProperties(props).build());
   }


   /**
    * Retrieves the configuration currently in use. The configuration object is immutable. If you wish to change configuration,
    * you should use the following pattern:
    *
    * <code>
    * ConfigurationBuilder builder = new ConfigurationBuilder();
    * builder.read(remoteCacheManager.getConfiguration());
    * // modify builder
    * remoteCacheManager.stop();
    * remoteCacheManager = new RemoteCacheManager(builder.build());
    * </code>
    *
    * @since 5.3
    * @return The configuration of this RemoteCacheManager
    */
   public Configuration getConfiguration() {
      return configuration;
   }

   /**
    * Retrieves a clone of the properties currently in use.  Note that making any changes to the properties instance
    * retrieved will not affect an already-running RemoteCacheManager.  If you wish to make changes to an already-running
    * RemoteCacheManager, you should use the following pattern:
    *
    * <code>
    * Properties p = remoteCacheManager.getProperties();
    * // update properties
    * remoteCacheManager.stop();
    * remoteCacheManager = new RemoteCacheManager(p);
    * </code>
    * @return a clone of the properties used to configure this RemoteCacheManager
    * @since 4.2
    */
   @Deprecated
   public Properties getProperties() {
      Properties properties = new Properties();
      if (configuration.asyncExecutorFactory().factoryClass() != null) {
         properties.setProperty(ConfigurationProperties.ASYNC_EXECUTOR_FACTORY, configuration.asyncExecutorFactory().factoryClass().getName());
         TypedProperties aefProps = configuration.asyncExecutorFactory().properties();
         for(String key : Arrays.asList(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_QUEUE_SIZE)) {
            if (aefProps.containsKey(key)) {
               properties.setProperty(key, aefProps.getProperty(key));
            }
         }
      }
      properties.setProperty(ConfigurationProperties.REQUEST_BALANCING_STRATEGY, configuration.balancingStrategy().getName());
      properties.setProperty(ConfigurationProperties.CONNECT_TIMEOUT, Integer.toString(configuration.connectionTimeout()));
      for (int i = 1; i <= configuration.consistentHashImpl().length; i++) {
         properties.setProperty(ConfigurationProperties.HASH_FUNCTION_PREFIX + "." + i, configuration.consistentHashImpl()[i-1].getName());
      }
      properties.setProperty(ConfigurationProperties.FORCE_RETURN_VALUES, Boolean.toString(configuration.forceReturnValues()));
      properties.setProperty(ConfigurationProperties.KEY_SIZE_ESTIMATE, Integer.toString(configuration.keySizeEstimate()));
      properties.setProperty(ConfigurationProperties.MARSHALLER, configuration.marshallerClass().getName());
      properties.setProperty(ConfigurationProperties.PING_ON_STARTUP, Boolean.toString(configuration.pingOnStartup()));
      properties.setProperty(ConfigurationProperties.PROTOCOL_VERSION, configuration.protocolVersion());
      properties.setProperty(ConfigurationProperties.SO_TIMEOUT, Integer.toString(configuration.socketTimeout()));
      properties.setProperty(ConfigurationProperties.TCP_NO_DELAY, Boolean.toString(configuration.tcpNoDelay()));
      properties.setProperty(ConfigurationProperties.TRANSPORT_FACTORY, configuration.transportFactory().getName());
      properties.setProperty(ConfigurationProperties.VALUE_SIZE_ESTIMATE, Integer.toString(configuration.valueSizeEstimate()));

      properties.setProperty("exhaustedAction", Integer.toString(configuration.connectionPool().exhaustedAction().ordinal()));
      properties.setProperty("maxActive", Integer.toString(configuration.connectionPool().maxActive()));
      properties.setProperty("maxTotal", Integer.toString(configuration.connectionPool().maxTotal()));
      properties.setProperty("maxWait", Long.toString(configuration.connectionPool().maxWait()));
      properties.setProperty("maxIdle", Integer.toString(configuration.connectionPool().maxIdle()));
      properties.setProperty("minIdle", Integer.toString(configuration.connectionPool().minIdle()));
      properties.setProperty("numTestsPerEvictionRun", Integer.toString(configuration.connectionPool().numTestsPerEvictionRun()));
      properties.setProperty("minEvictableIdleTimeMillis", Long.toString(configuration.connectionPool().minEvictableIdleTime()));
      properties.setProperty("timeBetweenEvictionRunsMillis", Long.toString(configuration.connectionPool().timeBetweenEvictionRuns()));

      properties.setProperty("lifo", Boolean.toString(configuration.connectionPool().lifo()));
      properties.setProperty("testOnBorrow", Boolean.toString(configuration.connectionPool().testOnBorrow()));
      properties.setProperty("testOnReturn", Boolean.toString(configuration.connectionPool().testOnReturn()));
      properties.setProperty("testWhileIdle", Boolean.toString(configuration.connectionPool().testWhileIdle()));

      StringBuilder servers = new StringBuilder();
      for(ServerConfiguration server : configuration.servers()) {
         if (servers.length() > 0) {
            servers.append(";");
         }
         servers.append(server.host()).append(":").append(server.port());
      }
      properties.setProperty(ConfigurationProperties.SERVER_LIST, servers.toString());

      return properties;
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in the
    * classpath, in a file named <tt>hotrod-client.properties</tt>. If no properties can be found in the classpath, the
    * server tries to connect to "127.0.0.1:11222" in start.
    *
    * @param start weather or not to start the RemoteCacheManager
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
      if (start) start();
   }

   /**
    * Same as {@link #RemoteCacheManager(boolean)} and it also starts the cache.
    */
   public RemoteCacheManager() {
      this(true);
   }

   /**
    * Creates a remote cache manager aware of the Hot Rod server listening at host:port.
    *
    * @param start weather or not to start the RemoteCacheManager.
    */
   @Deprecated
   public RemoteCacheManager(String host, int port, boolean start) {
	   this(host, port, start, Thread.currentThread().getContextClassLoader());
   }

   /**
    * Creates a remote cache manager aware of the Hot Rod server listening at host:port.
    *
    * @param start weather or not to start the RemoteCacheManager.
    */
   @Deprecated
   public RemoteCacheManager(String host, int port, boolean start, ClassLoader classLoader) {
      this(new ConfigurationBuilder().classLoader(classLoader).addServer().host(host).port(port).build(), start);
   }

   /**
    * Same as {@link #RemoteCacheManager(String, int, boolean)} with start=true.
    */
   @Deprecated
   public RemoteCacheManager(String host, int port) {
	   this(host, port, Thread.currentThread().getContextClassLoader());
   }

   /**
    * Same as {@link #RemoteCacheManager(String, int, boolean)} with start=true.
    */
   @Deprecated
   public RemoteCacheManager(String host, int port, ClassLoader classLoader) {
      this(host, port, true, classLoader);
   }

   /**
    * The given string should have the following structure: "host1:port2;host:port2...". Every host:port defines a
    * server.
    */
   @Deprecated
   public RemoteCacheManager(String servers, boolean start) {
	   this(servers, start, Thread.currentThread().getContextClassLoader());
   }

   /**
    * The given string should have the following structure: "host1:port2;host:port2...". Every host:port defines a
    * server.
    */
   @Deprecated
   public RemoteCacheManager(String servers, boolean start, ClassLoader classLoader) {
      this(new ConfigurationBuilder().classLoader(classLoader).addServers(servers).build(), start);
   }

   /**
    * Same as {@link #RemoteCacheManager(String, boolean)}, with start=true.
    */
   @Deprecated
   public RemoteCacheManager(String servers) {
	   this(servers, Thread.currentThread().getContextClassLoader());
   }

   /**
    * Same as {@link #RemoteCacheManager(String, boolean)}, with start=true.
    */
   @Deprecated
   public RemoteCacheManager(String servers, ClassLoader classLoader) {
      this(servers, true, classLoader);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in
    * supplied URL.
    *
    * @param start weather or not to start the RemoteCacheManager
    * @throws HotRodClientException if properties could not be loaded
    */
   @Deprecated
   public RemoteCacheManager(URL config, boolean start) {
	   this(config, start, Thread.currentThread().getContextClassLoader());
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in
    * supplied URL.
    *
    * @param start weather or not to start the RemoteCacheManager
    * @throws HotRodClientException if properties could not be loaded
    */
   @Deprecated
   public RemoteCacheManager(URL config, boolean start, ClassLoader classLoader) {

      InputStream stream = null;
      try {
         stream = config.openStream();
         Properties properties = loadFromStream(stream);
         configuration = new ConfigurationBuilder().classLoader(classLoader).withProperties(properties).build();
      } catch (IOException e) {
         throw new HotRodClientException("Could not read URL:" + config, e);
      } finally {
         try {
            if (stream != null)
               stream.close();
         } catch (IOException e) {
            // ignore
         }
      }
      if (start)
         start();
   }

   /**
    * Same as {@link #RemoteCacheManager(java.net.URL)} and it also starts the cache (start==true).
    *
    * @param config
    */
   @Deprecated
   public RemoteCacheManager(URL config) {
	   this(config, Thread.currentThread().getContextClassLoader());
   }

   /**
    * Same as {@link #RemoteCacheManager(java.net.URL)} and it also starts the cache (start==true).
    *
    * @param config
    */
   @Deprecated
   public RemoteCacheManager(URL config, ClassLoader classLoader) {
      this(config, true, classLoader);
   }

   /**
    * Retrieves a named cache from the remote server if the cache has been
    * defined, otherwise if the cache name is underfined, it will return null.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName or null if the cache
    *         name has not been defined
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache(String cacheName) {
      return getCache(cacheName, configuration.forceReturnValues());
   }

   public <K, V> RemoteCache<K, V> getCache(String cacheName, boolean forceReturnValue) {
      return createRemoteCache(cacheName, forceReturnValue);
   }

   /**
    * Retrieves the default cache from the remote server.
    *
    * @return a remote cache instance that can be used to send requests to the
    *         default cache in the server
    */
   @Override
   public <K, V> RemoteCache<K, V> getCache() {
      return getCache(configuration.forceReturnValues());
   }

   public <K, V> RemoteCache<K, V> getCache(boolean forceReturnValue) {
      //As per the HotRod protocol specification, the default cache is identified by an empty string
      return createRemoteCache("", forceReturnValue);
   }

   @Override
   public void start() {
      // Workaround for JDK6 NPE: http://bugs.sun.com/view_bug.do?bug_id=6427854
      SysPropertyActions.setProperty("sun.nio.ch.bugLevel", "\"\"");

      codec = CodecFactory.getCodec(configuration.protocolVersion());

      transportFactory = Util.getInstance(configuration.transportFactory());

      transportFactory.start(codec, configuration, topologyId);
      if (marshaller == null) {
         marshaller = configuration.marshaller();
         if (marshaller == null) {
            marshaller = Util.getInstance(configuration.marshallerClass());
         }
      }

      if (asyncExecutorService == null) {
         ExecutorFactory executorFactory = configuration.asyncExecutorFactory().factory();
         if (executorFactory == null) {
            executorFactory = Util.getInstance(configuration.asyncExecutorFactory().factoryClass());
         }
         asyncExecutorService = executorFactory.getExecutor(configuration.asyncExecutorFactory().properties());
      }

      synchronized (cacheName2RemoteCache) {
         for (RemoteCacheHolder rcc : cacheName2RemoteCache.values()) {
            startRemoteCache(rcc);
         }
      }

      // Print version to help figure client version run
      log.version(org.infinispan.Version.printVersion());

      started = true;
   }

   @Override
   public void stop() {
      if (isStarted()) {
         transportFactory.destroy();
         asyncExecutorService.shutdownNow();
      }
      started = false;
   }

   public boolean isStarted() {
      return started;
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
         if (!cacheName2RemoteCache.containsKey(cacheName)) {
            RemoteCacheImpl<K, V> result = new RemoteCacheImpl<K, V>(this, cacheName);
            RemoteCacheHolder rcc = new RemoteCacheHolder(result, forceReturnValueOverride == null ? configuration.forceReturnValues() : forceReturnValueOverride);
            startRemoteCache(rcc);
            if (configuration.pingOnStartup()) {
               // If ping not successful assume that the cache does not exist
               // Default cache is always started, so don't do for it
               if (!cacheName.equals(BasicCacheContainer.DEFAULT_CACHE_NAME) &&
                     ping(result) == PingResult.CACHE_DOES_NOT_EXIST) {
                  return null;
               }
            }
            // If ping on startup is disabled, or cache is defined in server
            cacheName2RemoteCache.put(cacheName, rcc);
            return result;
         } else {
            return (RemoteCache<K, V>) cacheName2RemoteCache.get(cacheName).remoteCache;
         }
      }
   }

   private <K, V> PingResult ping(RemoteCacheImpl<K, V> cache) {
      if (transportFactory == null) {
         return PingResult.FAIL;
      }

      return cache.ping();
   }

   private void startRemoteCache(RemoteCacheHolder remoteCacheHolder) {
      RemoteCacheImpl<?, ?> remoteCache = remoteCacheHolder.remoteCache;
      OperationsFactory operationsFactory = new OperationsFactory(
            transportFactory, remoteCache.getName(), topologyId, remoteCacheHolder.forceReturnValue, codec);
      remoteCache.init(marshaller, asyncExecutorService, operationsFactory, configuration.keySizeEstimate(), configuration.valueSizeEstimate());
   }

   public Marshaller getMarshaller() {
      return marshaller;
   }
}

class RemoteCacheHolder {
   final RemoteCacheImpl<?, ?> remoteCache;
   final boolean forceReturnValue;

   RemoteCacheHolder(RemoteCacheImpl<?, ?> remoteCache, boolean forceReturnValue) {
      this.remoteCache = remoteCache;
      this.forceReturnValue = forceReturnValue;
   }
}
