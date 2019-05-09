package org.infinispan.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;

import org.infinispan.Version;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.admin.ServerAdminOperationsHandler;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
public class Server {
   public static final Log log = LogFactory.getLog("SERVER", Log.class);

   // Properties
   public static final String INFINISPAN_BIND_ADDRESS = "infinispan.bind.address";
   public static final String INFINISPAN_CLUSTER_NAME = "infinispan.cluster.name";
   public static final String INFINISPAN_NODE_NAME = "infinispan.node.name";
   public static final String INFINISPAN_PORT_OFFSET = "infinispan.socket.binding.port-offset";
   /**
    * Property name indicating the path to the server installation. If unspecified, the current working directory will be used
    */
   public static final String INFINISPAN_SERVER_HOME_PATH = "infinispan.server.home.path";
   /**
    * Property name indicating the path to the root of a server instance. If unspecified, defaults to the <i>server</i> directory under the server home.
    */
   public static final String INFINISPAN_SERVER_ROOT_PATH = "infinispan.server.root.path";
   /**
    * Property name indicating the path to the configuration directory of a server instance. If unspecified, defaults to the <i>conf</i> directory under the server root.
    */
   public static final String INFINISPAN_SERVER_CONFIG_PATH = "infinispan.server.config.path";
   /**
    * Property name indicating the path to the data directory of a server instance. If unspecified, defaults to the <i>data</i> directory under the server root.
    */
   public static final String INFINISPAN_SERVER_DATA_PATH = "infinispan.server.data.path";
   /**
    * Property name indicating the path to the log directory of a server instance. If unspecified, defaults to the <i>log</i> directory under the server root.
    */
   public static final String INFINISPAN_SERVER_LOG_PATH = "infinispan.server.log.path";

   // Defaults
   private static final String SERVER_DEFAULTS = "infinispan-defaults.xml";
   public static final String DEFAULT_SERVER_CONFIG = "conf";
   public static final String DEFAULT_SERVER_DATA = "data";
   public static final String DEFAULT_SERVER_LIB = "lib";
   public static final String DEFAULT_SERVER_LOG = "log";
   public static final String DEFAULT_SERVER_ROOT_DIR = "server";
   public static final String DEFAULT_CONFIGURATION_FILE = "infinispan.xml";

   private final TimeService timeService;
   private final File serverRoot;
   private final File serverConf;
   private final long startTime;
   private final Properties properties;
   private ExitHandler exitHandler = new DefaultExitHandler();
   private ConfigurationBuilderHolder defaultsHolder;
   private ConfigurationBuilderHolder configurationBuilderHolder;
   private Map<String, DefaultCacheManager> cacheManagers;
   private Map<String, ProtocolServer> protocolServers;
   private volatile boolean running = false;
   private ComponentStatus status;

   /**
    * Initializes a server with the default server root, the default configuration file and system properties
    */
   public Server() {
      this(
            new File(DEFAULT_SERVER_ROOT_DIR),
            new File(DEFAULT_CONFIGURATION_FILE),
            SecurityActions.getSystemProperties()
      );
   }

   /**
    * Initializes a server with the supplied server root, configuration file and properties
    *
    * @param serverRoot
    * @param configuration
    * @param properties
    */
   public Server(File serverRoot, File configuration, Properties properties) {
      this(serverRoot, properties);
      if (!configuration.isAbsolute()) {
         configuration = new File(serverConf, configuration.getPath());
      }
      try (InputStream is = new FileInputStream(configuration)) {
         parseConfiguration(is);
      } catch (IOException e) {
         throw new CacheConfigurationException(e);
      }
   }

   private Server(File serverRoot, Properties properties) {
      this.timeService = DefaultTimeService.INSTANCE;
      this.startTime = timeService.time();
      this.serverRoot = serverRoot;
      this.properties = properties;
      this.status = ComponentStatus.INSTANTIATED;

      // Populate system properties unless they have already been set externally
      properties.putIfAbsent(INFINISPAN_SERVER_ROOT_PATH, serverRoot);
      properties.putIfAbsent(INFINISPAN_SERVER_CONFIG_PATH, new File(serverRoot, DEFAULT_SERVER_CONFIG).getAbsolutePath());
      properties.putIfAbsent(INFINISPAN_SERVER_DATA_PATH, new File(serverRoot, DEFAULT_SERVER_DATA).getAbsolutePath());
      properties.putIfAbsent(INFINISPAN_SERVER_LOG_PATH, new File(serverRoot, DEFAULT_SERVER_LOG).getAbsolutePath());

      this.serverConf = new File(properties.getProperty(INFINISPAN_SERVER_CONFIG_PATH));
   }

   private void parseConfiguration(InputStream config) {
      ParserRegistry parser = new ParserRegistry(this.getClass().getClassLoader(), false, properties);
      try (InputStream defaults = this.getClass().getClassLoader().getResourceAsStream(SERVER_DEFAULTS)) {
         // load the defaults first
         defaultsHolder = parser.parse(defaults);

         // base the global configuration to the default
         configurationBuilderHolder = new ConfigurationBuilderHolder();
         configurationBuilderHolder.getGlobalConfigurationBuilder().read(defaultsHolder.getGlobalConfigurationBuilder().build());

         // Copy all default templates
         for (Map.Entry<String, ConfigurationBuilder> entry : defaultsHolder.getNamedConfigurationBuilders().entrySet()) {
            configurationBuilderHolder.newConfigurationBuilder(entry.getKey()).read(entry.getValue().build());
         }

         // then load the user configuration
         configurationBuilderHolder = parser.parse(config, configurationBuilderHolder);

         // Set the operation handler on all endpoints
         ServerAdminOperationsHandler adminOperationsHandler = new ServerAdminOperationsHandler(defaultsHolder);
         ServerConfigurationBuilder serverConfigurationBuilder = configurationBuilderHolder.getGlobalConfigurationBuilder().module(ServerConfigurationBuilder.class);
         serverConfigurationBuilder.endpoints().forEach(builder -> builder.adminOperationsHandler(adminOperationsHandler));

         // Amend the named caches configurations with the defaults
         for (Map.Entry<String, ConfigurationBuilder> entry : configurationBuilderHolder.getNamedConfigurationBuilders().entrySet()) {
            Configuration cfg = entry.getValue().build();
            ConfigurationBuilder defaultCfg = defaultsHolder.getNamedConfigurationBuilders().get("org.infinispan." + cfg.clustering().cacheMode().name());
            ConfigurationBuilder rebased = new ConfigurationBuilder().read(defaultCfg.build());
            rebased.read(cfg);
            entry.setValue(rebased);
         }

         configurationBuilderHolder.validate();
      } catch (IOException | XMLStreamException e) {
         throw new CacheConfigurationException(e);
      }
   }

   public ExitHandler getExitHandler() {
      return exitHandler;
   }

   public void setExitHandler(ExitHandler exitHandler) {
      if (!running) {
         this.exitHandler = exitHandler;
      } else {
         throw new IllegalStateException("Cannot change exit handler on a running server");
      }
   }

   public CompletableFuture<Integer> run() {
      CompletableFuture<Integer> r = exitHandler.getExitFuture();
      cacheManagers = new LinkedHashMap<>(1);
      protocolServers = new LinkedHashMap<>(3);
      try {
         // Start the cache manager(s)
         DefaultCacheManager cm = new DefaultCacheManager(configurationBuilderHolder, true);
         cacheManagers.put(cm.getName(), cm);

         // Start the protocol servers
         ServerConfiguration serverConfiguration = cm.getCacheManagerConfiguration().module(ServerConfiguration.class);
         // We can start the protocol servers in parallel
         serverConfiguration.endpoints().parallelStream().forEach(configuration -> {
            Class<? extends ProtocolServer> protocolServerClass = configuration.getClass().getAnnotation(ConfigurationFor.class).value().asSubclass(ProtocolServer.class);
            ProtocolServer protocolServer = Util.getInstance(protocolServerClass);
            protocolServers.put(protocolServer.getName() + "-" + configuration.name(), protocolServer);
            protocolServer.start(configuration, cm);
            ProtocolServerConfiguration protocolConfig = protocolServer.getConfiguration();
            log.protocolStarted(protocolServer.getName(), protocolConfig.host(), protocolConfig.port());
         });
         this.status = ComponentStatus.RUNNING;
         log.serverStarted(Version.getBrandName(), Version.getVersion(), timeService.timeDuration(startTime, TimeUnit.MILLISECONDS));
      } catch (Exception e) {
         r.completeExceptionally(e);
      }
      r = r.whenComplete((status, t) -> {
         shutdown();
      });
      return r;
   }

   private void shutdown() {
      status = ComponentStatus.STOPPING;
      // Shutdown the protocol servers in parallel
      protocolServers.values().parallelStream().forEach(ps -> ps.stop());
      cacheManagers.values().forEach(cm -> cm.stop());
      status = ComponentStatus.TERMINATED;
   }

   public ConfigurationBuilderHolder getConfigurationBuilderHolder() {
      return configurationBuilderHolder;
   }

   public File getServerRoot() {
      return serverRoot;
   }

   public Map<String, DefaultCacheManager> getCacheManagers() {
      return cacheManagers;
   }

   public Map<String, ProtocolServer> getProtocolServers() {
      return protocolServers;
   }
}
