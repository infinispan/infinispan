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
package org.infinispan.server.core

import java.net.InetSocketAddress
import org.infinispan.manager.EmbeddedCacheManager
import transport.NettyTransport
import org.infinispan.util.{ClusterIdGenerator, TypedProperties, FileLookupFactory}
import logging.Log
import org.infinispan.jmx.{JmxUtil, ResourceDMBean}
import javax.management.{ObjectName, MBeanServer}
import java.util.Properties
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import javax.net.ssl.KeyManager
import javax.net.ssl.TrustManager
import org.infinispan.server.core.transport.LifecycleChannelPipelineFactory
import org.infinispan.server.core.transport.TimeoutEnabledChannelPipelineFactory
import org.infinispan.server.core.transport.NettyChannelPipelineFactory

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
   protected var versionGenerator: ClusterIdGenerator = _
   private var transportObjName: ObjectName = _
   private var mbeanServer: MBeanServer = _
   private var isGlobalStatsEnabled: Boolean = _

   def start(configuration: SuitableConfiguration, cacheManager: EmbeddedCacheManager) {
      this.configuration = configuration
      this.cacheManager = cacheManager
      this.isGlobalStatsEnabled = cacheManager.getCacheManagerConfiguration.globalJmxStatistics().enabled()

      if (isDebugEnabled) {
         debugf("Starting server with configuration: %s", configuration)
      }

      // Start default cache
      startDefaultCache

      this.versionGenerator = new ClusterIdGenerator(
         cacheManager, cacheManager.getTransport)

      startTransport()
   }

   def startTransport() {
      val address = new InetSocketAddress(configuration.host, configuration.port)
      transport = new NettyTransport(this, getPipeline, address, configuration, getQualifiedName, cacheManager)

      // Register transport MBean regardless
      registerTransportMBean()

      transport.start()
   }

   override def getPipeline: LifecycleChannelPipelineFactory = {
      if (configuration.idleTimeout > 0)
         new TimeoutEnabledChannelPipelineFactory(this, getEncoder)
      else // Idle timeout logic is disabled with -1 or 0 values
         new NettyChannelPipelineFactory(this, getEncoder)
   }

   protected def registerTransportMBean() {
      val globalCfg = cacheManager.getCacheManagerConfiguration
      mbeanServer = JmxUtil.lookupMBeanServer(globalCfg)
      val groupName = "type=Server,name=%s".format(getQualifiedName)
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

   def startDefaultCache = cacheManager.getCache[AnyRef, AnyRef]()
}
