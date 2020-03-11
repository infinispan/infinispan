package org.infinispan.server.core;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.eclipse.microprofile.metrics.MetricID;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metrics.impl.CacheManagerMetricsRegistration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.core.utils.ManageableThreadPoolExecutorService;
import org.infinispan.tasks.TaskManager;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * A common protocol server dealing with common property parameter validation and assignment and transport lifecycle.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
public abstract class AbstractProtocolServer<C extends ProtocolServerConfiguration> implements ProtocolServer<C> {

   private static final Log log = LogFactory.getLog(AbstractProtocolServer.class, Log.class);

   private final String protocolName;

   protected NettyTransport transport;
   protected EmbeddedCacheManager cacheManager;
   protected C configuration;
   private CacheIgnoreManager cacheIgnore;
   private ObjectName transportObjName;
   private CacheManagerJmxRegistration jmxRegistration;
   private ThreadPoolExecutor executor;
   private ManageableThreadPoolExecutorService manageableThreadPoolExecutorService;
   private ObjectName executorObjName;
   private CacheManagerMetricsRegistration metricsRegistration;
   private Set<MetricID> metricIds;

   protected AbstractProtocolServer(String protocolName) {
      this.protocolName = protocolName;
   }

   @Override
   public String getName() {
      return protocolName;
   }

   protected void startInternal() {
      registerAdminOperationsHandler();

      // Start default cache
      startCaches();

      if (configuration.startTransport())
         startTransport();
   }

   private void registerAdminOperationsHandler() {
      if (configuration.adminOperationsHandler() != null) {
         TaskManager taskManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(TaskManager.class);
         if (taskManager != null) {
            taskManager.registerTaskEngine(configuration.adminOperationsHandler());
         } else {
            throw log.cannotRegisterAdminOperationsHandler();
         }
      }
   }

   protected boolean isCacheIgnored(String cache) {
      return cacheIgnore.isCacheIgnored(cache);
   }

   public CacheIgnoreManager getCacheIgnore() {
      return cacheIgnore;
   }

   @Override
   public void start(C configuration, EmbeddedCacheManager cacheManager) {
      if (log.isDebugEnabled()) {
         log.debugf("Starting server with configuration: %s", configuration);
      }

      this.configuration = configuration;
      this.cacheManager = cacheManager;

      BasicComponentRegistry bcr = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(BasicComponentRegistry.class.getName());
      cacheIgnore = bcr.getComponent(CacheIgnoreManager.class).running();
      if (cacheIgnore == null) {
         throw new IllegalStateException("CacheIgnoreManager is a required component");
      }

      executor = new ThreadPoolExecutor(
            configuration.workerThreads(),
            configuration.workerThreads(),
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new DefaultThreadFactory(getQualifiedName() + "-ServerHandler"),
            new ThreadPoolExecutor.AbortPolicy() {
               @Override
               public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                  if (e.isShutdown())
                     throw new IllegalLifecycleStateException("Server has been stopped");
                  else
                     super.rejectedExecution(r, e);
               }
            });

      manageableThreadPoolExecutorService = new ManageableThreadPoolExecutorService(executor);

      try {
         startInternal();
      } catch (RuntimeException t) {
         stop();
         throw t;
      }
   }

   protected void startTransport() {
      log.debugf("Starting Netty transport on %s:%s", configuration.host(), configuration.port());
      InetSocketAddress address = new InetSocketAddress(configuration.host(), configuration.port());
      transport = new NettyTransport(address, configuration, getQualifiedName(), cacheManager);
      transport.initializeHandler(getInitializer());

      // Register transport and worker MBeans regardless
      registerServerMBeans();

      try {
         transport.start();
      } catch (Throwable re) {
         try {
            unregisterServerMBeans();
         } catch (Exception e) {
            throw new CacheException(e);
         }
         throw re;
      }

      registerMetrics();
   }

   public ThreadPoolExecutor getExecutor() {
      return executor;
   }

   protected void registerServerMBeans() {
      GlobalConfiguration globalCfg = SecurityActions.getCacheManagerConfiguration(cacheManager);
      if (globalCfg.jmx().enabled()) {
         jmxRegistration = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(CacheManagerJmxRegistration.class);
         String groupName = String.format("type=Server,name=%s-%d", getQualifiedName(), configuration.port());
         try {
            transportObjName = jmxRegistration.registerExternalMBean(transport, groupName);
            executorObjName = jmxRegistration.registerExternalMBean(manageableThreadPoolExecutorService, groupName);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   protected void unregisterServerMBeans() throws Exception {
      if (transportObjName != null) {
         jmxRegistration.unregisterMBean(transportObjName);
      }
      if (executorObjName != null) {
         jmxRegistration.unregisterMBean(executorObjName);
      }
   }

   protected void registerMetrics() {
      metricsRegistration = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(CacheManagerMetricsRegistration.class);
      if (metricsRegistration.metricsEnabled()) {
         String protocol = "server_" + getQualifiedName() + '_' + configuration.port();
         metricIds = Collections.synchronizedSet(metricsRegistration.registerExternalMetrics(transport, protocol));
         metricIds.addAll(metricsRegistration.registerExternalMetrics(manageableThreadPoolExecutorService, protocol));
      }
   }

   protected void unregisterMetrics() {
      if (metricIds != null) {
         metricsRegistration.unregisterMetrics(metricIds);
         metricIds = null;
      }
   }

   public final String getQualifiedName() {
      return protocolName + (configuration.name().length() > 0 ? "-" : "") + configuration.name();
   }

   @Override
   public void stop() {
      boolean isDebug = log.isDebugEnabled();
      if (isDebug && configuration != null)
         log.debugf("Stopping server %s listening at %s:%d", getQualifiedName(), configuration.host(), configuration.port());

      if (executor != null)
         executor.shutdownNow();

      if (transport != null)
         transport.stop();

      try {
         unregisterServerMBeans();
      } catch (Exception e) {
         throw new CacheException(e);
      }

      unregisterMetrics();

      if (isDebug)
         log.debug("Server stopped");
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public String getHost() {
      return configuration.host();
   }

   public Integer getPort() {
      if (transport != null) {
         return transport.getPort();
      }
      return configuration.port();
   }

   @Override
   public C getConfiguration() {
      return configuration;
   }

   protected void startCaches() {
      // DefaultCacheManager already starts all the defined/persisted/global state caches
      // But the default cache may not be defined (e.g. it might be using a wildcard template)
      String name = defaultCacheName();
      if (name != null) {
         log.debugf("Starting default cache: %s", configuration.defaultCacheName());
         cacheManager.getCache(name);
      } else {
         log.debugf("No default cache to start");
      }
   }

   public String defaultCacheName() {
      if (configuration.defaultCacheName() != null) {
         return configuration.defaultCacheName();
      } else {
         return SecurityActions.getCacheManagerConfiguration(cacheManager).defaultCacheName().orElse(null);
      }
   }

   public boolean isTransportEnabled() {
      return transport != null;
   }

   public NettyTransport getTransport() {
      return transport;
   }
}
