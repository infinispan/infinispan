package org.infinispan.server.core

import java.net.InetSocketAddress
import org.infinispan.manager.EmbeddedCacheManager
import transport.NettyTransport
import logging.Log
import org.infinispan.jmx.{JmxUtil, ResourceDMBean}
import javax.management.{ObjectName, MBeanServer}
import org.infinispan.server.core.transport.TimeoutEnabledChannelInitializer
import org.infinispan.server.core.transport.NettyChannelInitializer
import io.netty.channel.{Channel, ChannelInitializer}

/**
 * A common protocol server dealing with common property parameter validation and assignment and transport lifecycle.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolServer(protocolName: String) extends ProtocolServer with Log {
   protected var transport: NettyTransport = _
   protected var cacheManager: EmbeddedCacheManager = _
   protected var configuration: SuitableConfiguration = null.asInstanceOf[SuitableConfiguration]
   private var transportObjName: ObjectName = _
   private var mbeanServer: MBeanServer = _
   private var isGlobalStatsEnabled: Boolean = _

   protected def startInternal(configuration: SuitableConfiguration, cacheManager: EmbeddedCacheManager) {
      this.configuration = configuration
      this.cacheManager = cacheManager
      this.isGlobalStatsEnabled = cacheManager.getCacheManagerConfiguration.globalJmxStatistics().enabled()

      if (isDebugEnabled) {
         debugf("Starting server with configuration: %s", configuration)
      }

      // Start default cache
      startDefaultCache

      startTransport()
   }

   final override def start(configuration: SuitableConfiguration, cacheManager: EmbeddedCacheManager) {
      try {
         startInternal(configuration, cacheManager)
      } catch {
         case t: Throwable => {
            stop
            throw t
         }
      }
   }

   def startTransport() {
      val address = new InetSocketAddress(configuration.host, configuration.port)
      transport = new NettyTransport(this, getInitializer, address, configuration, getQualifiedName(), cacheManager)

      // Register transport MBean regardless
      registerTransportMBean()

      transport.start()
   }

   override def getInitializer: ChannelInitializer[Channel] = {
      if (configuration.idleTimeout > 0)
         new TimeoutEnabledChannelInitializer(this, getEncoder)
      else // Idle timeout logic is disabled with -1 or 0 values
         new NettyChannelInitializer(this, getEncoder)
   }

   protected def registerTransportMBean() {
      val globalCfg = cacheManager.getCacheManagerConfiguration
      mbeanServer = JmxUtil.lookupMBeanServer(globalCfg)
      val groupName = "type=Server,name=%s".format(getQualifiedName())
      val jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mbeanServer, groupName)

      // Pick up metadata from the component metadata repository
      val meta = LifecycleCallbacks.componentMetadataRepo
              .findComponentMetadata(transport.getClass).toManageableComponentMetadata
      // And use this metadata when registering the transport as a dynamic MBean
      val dynamicMBean = new ResourceDMBean(transport, meta)

      transportObjName = new ObjectName(
         "%s:%s,component=%s".format(jmxDomain, groupName, meta.getJmxObjectName))
      JmxUtil.registerMBean(dynamicMBean, transportObjName, mbeanServer)
   }

   protected def unregisterTransportMBean() {
      if (mbeanServer != null && transportObjName != null) {
         // Unregister mbean(s)
         JmxUtil.unregisterMBean(transportObjName, mbeanServer)
      }
   }

   private def getQualifiedName(): String = {
      protocolName + (if (configuration.name.length > 0) "-" else "") + configuration.name
   }

   override def stop {
      val isDebug = isDebugEnabled
      if (isDebug && configuration != null)
         debug("Stopping server listening in %s:%d", configuration.host, configuration.port)

      if (transport != null)
         transport.stop()

      unregisterTransportMBean()

      if (isDebug)
         debug("Server stopped")
   }

   def getCacheManager = cacheManager

   def getConfiguration = configuration

   def getHost = configuration.host

   def getPort: Int = configuration.port

   def startDefaultCache = cacheManager.getCache[AnyRef, AnyRef](configuration.defaultCacheName)
}
