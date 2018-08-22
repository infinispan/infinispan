package org.infinispan.server.core;

import java.net.InetSocketAddress;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.tasks.TaskManager;

/**
 * A common protocol server dealing with common property parameter validation and assignment and transport lifecycle.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
public abstract class AbstractProtocolServer<A extends ProtocolServerConfiguration> extends AbstractCacheIgnoreAware
      implements ProtocolServer<A> {

   private static final Log log = LogFactory.getLog(AbstractProtocolServer.class, Log.class);

   private final String protocolName;

   protected NettyTransport transport;
   protected EmbeddedCacheManager cacheManager;
   protected A configuration;
   private ObjectName transportObjName;
   private MBeanServer mbeanServer;


   protected AbstractProtocolServer(String protocolName) {
      this.protocolName = protocolName;
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

      if(configuration.startTransport())
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

   @Override
   public final void start(A configuration, EmbeddedCacheManager cacheManager) {
      try {
         configuration.ignoredCaches().forEach(this::ignoreCache);
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

      // Register transport MBean regardless
      registerTransportMBean();

      try {
         transport.start();
      } catch (Throwable re) {
         try {
            unregisterTransportMBean();
         } catch (Exception e) {
            throw new CacheException(e);
         }
         throw re;
      }
   }

   protected void registerTransportMBean() {
      GlobalConfiguration globalCfg = cacheManager.getCacheManagerConfiguration();
      GlobalJmxStatisticsConfiguration jmxConfig = globalCfg.globalJmxStatistics();
      mbeanServer = JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
      String groupName = String.format("type=Server,name=%s", getQualifiedName());
      String jmxDomain = JmxUtil.buildJmxDomain(jmxConfig.domain(), mbeanServer, groupName);

      // Pick up metadata from the component metadata repository
      ManageableComponentMetadata meta = LifecycleCallbacks.componentMetadataRepo
            .findComponentMetadata(transport.getClass()).toManageableComponentMetadata();
      try {
         // And use this metadata when registering the transport as a dynamic MBean
         DynamicMBean dynamicMBean = new ResourceDMBean(transport, meta);

         transportObjName = new ObjectName(String.format("%s:%s,component=%s", jmxDomain, groupName,
               meta.getJmxObjectName()));
         JmxUtil.registerMBean(dynamicMBean, transportObjName, mbeanServer);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected void unregisterTransportMBean() throws Exception {
      if (mbeanServer != null && transportObjName != null) {
         // Unregister mbean(s)
         JmxUtil.unregisterMBean(transportObjName, mbeanServer);
      }
   }

   public String getQualifiedName() {
      return protocolName + (configuration.name().length() > 0 ? "-" : "") + configuration.name();
   }

   @Override
   public void stop() {
      boolean isDebug = log.isDebugEnabled();
      if (isDebug && configuration != null)
         log.debugf("Stopping server listening in %s:%d", configuration.host(), configuration.port());

      if (transport != null)
         transport.stop();

      try {
         unregisterTransportMBean();
      } catch (Exception e) {
         throw new CacheException(e);
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
      cacheManager.getCache(configuration.defaultCacheName());
   }

   public boolean isTransportEnabled() {
      return transport != null;
   }

   public NettyTransport getTransport() {
      return transport;
   }

   public abstract int getWorkerThreads();
}
