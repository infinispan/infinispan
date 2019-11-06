package org.infinispan.server.core;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.manager.EmbeddedCacheManager;
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
public abstract class AbstractProtocolServer<A extends ProtocolServerConfiguration> implements ProtocolServer<A> {

   private static final Log log = LogFactory.getLog(AbstractProtocolServer.class, Log.class);

   private final String protocolName;

   protected NettyTransport transport;
   protected EmbeddedCacheManager cacheManager;
   protected A configuration;
   private CacheIgnoreManager cacheIgnore;
   private ObjectName transportObjName;
   private CacheManagerJmxRegistration jmxRegistration;
   private ThreadPoolExecutor executor;
   private ObjectName executorObjName;

   protected AbstractProtocolServer(String protocolName) {
      this.protocolName = protocolName;
   }

   @Override
   public String getName() {
      return protocolName;
   }

   protected void startInternal(A configuration, EmbeddedCacheManager cacheManager) {
      this.configuration = configuration;
      this.cacheManager = cacheManager;

      if (log.isDebugEnabled()) {
         log.debugf("Starting server with configuration: %s", configuration);
      }

      registerAdminOperationsHandler();

      // Start default cache
      startDefaultCache();

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
   public final void start(A configuration, EmbeddedCacheManager cacheManager) {
      start(configuration, cacheManager, new CacheIgnoreManager(cacheManager));
   }

   @Override
   public void start(A configuration, EmbeddedCacheManager cacheManager, CacheIgnoreManager cacheIgnore) {
      this.cacheIgnore = cacheIgnore;
      try {
         startInternal(configuration, cacheManager);
      } catch (RuntimeException t) {
         stop();
         throw t;
      }
   }

   protected void startTransport() {
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
   }

   public ThreadPoolExecutor getExecutor() {
      if (this.executor == null || this.executor.isShutdown()) {
         DefaultThreadFactory factory = new DefaultThreadFactory(getQualifiedName() + "-ServerHandler");
         int workerThreads = configuration.workerThreads();
         this.executor = new ThreadPoolExecutor(
               workerThreads,
               workerThreads,
               0L, TimeUnit.MILLISECONDS,
               new LinkedBlockingQueue<>(),
               factory,
               abortPolicy);
      }
      return executor;
   }

   private ThreadPoolExecutor.AbortPolicy abortPolicy = new ThreadPoolExecutor.AbortPolicy() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
         if (executor.isShutdown())
            throw new IllegalLifecycleStateException("Server has been stopped");
         else
            super.rejectedExecution(r, e);
      }
   };

   protected void registerServerMBeans() {
      GlobalConfiguration globalCfg = SecurityActions.getCacheManagerConfiguration(cacheManager);
      GlobalJmxStatisticsConfiguration jmxConfig = globalCfg.globalJmxStatistics();
      if (jmxConfig.enabled()) {
         jmxRegistration = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(CacheManagerJmxRegistration.class);
         String groupName = String.format("type=Server,name=%s-%d", getQualifiedName(), configuration.port());
         try {
            transportObjName = jmxRegistration.registerExternalMBean(transport, groupName);
            executorObjName = jmxRegistration.registerExternalMBean(new ManageableThreadPoolExecutorService(getExecutor()), groupName);
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

   public final String getQualifiedName() {
      return protocolName + (configuration.name().length() > 0 ? "-" : "") + configuration.name();
   }

   @Override
   public void stop() {
      boolean isDebug = log.isDebugEnabled();
      if (isDebug && configuration != null)
         log.debugf("Stopping server %s listening at %s:%d", getQualifiedName(), configuration.host(), configuration.port());

      if (executor != null) executor.shutdownNow();

      if (transport != null)
         transport.stop();

      try {
         unregisterServerMBeans();
      } catch (Exception e) {
         throw new CacheException(e);
      }

      if (cacheIgnore != null) {
         cacheIgnore.stop();
      }

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
   public A getConfiguration() {
      return configuration;
   }

   protected void startDefaultCache() {
      String name = defaultCacheName();
      if (name != null) {
         cacheManager.getCache(name);
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
