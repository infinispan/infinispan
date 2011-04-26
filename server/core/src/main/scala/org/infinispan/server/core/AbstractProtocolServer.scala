/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.server.core.VersionGenerator._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util.Properties
import org.infinispan.util.{TypedProperties, Util}
import transport.NettyTransport

/**
 * A common protocol server dealing with common property parameter validation and assignment and transport lifecycle.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolServer(threadNamePrefix: String) extends ProtocolServer with Logging {
   protected var host: String = _
   protected var port: Int = _
   protected var workerThreads: Int = _
   protected var transport: NettyTransport = _
   protected var cacheManager: EmbeddedCacheManager = _

   def start(properties: Properties, cacheManager: EmbeddedCacheManager, defaultPort: Int) {
      val typedProps = TypedProperties.toTypedProperties(properties)
      // Enabled added to make it easy to enable/disable endpoints in JBoss MC based beans for EDG
      val toStart = typedProps.getBooleanProperty("enabled", true, true)

      if (toStart) {
         // By doing parameter validation here, both programmatic and command line clients benefit from it.
         this.host = typedProps.getProperty(PROP_KEY_HOST, HOST_DEFAULT, true)
         this.port = typedProps.getIntProperty(PROP_KEY_PORT, defaultPort, true)
         val masterThreads = typedProps.getIntProperty(PROP_KEY_MASTER_THREADS, -1, true)
         if (masterThreads != -1)
            warn("Setting the number of master threads is no longer supported")

         this.workerThreads = typedProps.getIntProperty(PROP_KEY_WORKER_THREADS, WORKER_THREADS_DEFAULT, true)
         if (workerThreads < 0)
            throw new IllegalArgumentException("Worker threads can't be lower than 0: " + masterThreads)

         this.cacheManager = cacheManager
         val idleTimeout = typedProps.getIntProperty(PROP_KEY_IDLE_TIMEOUT, IDLE_TIMEOUT_DEFAULT, true)
         if (idleTimeout < -1)
            throw new IllegalArgumentException("Idle timeout can't be lower than -1: " + idleTimeout)

         val tcpNoDelay = typedProps.getBooleanProperty(PROP_KEY_TCP_NO_DELAY, TCP_NO_DELAY_DEFAULT, true)

         val sendBufSize = typedProps.getIntProperty(PROP_KEY_SEND_BUF_SIZE, SEND_BUF_SIZE_DEFAULT, true)
         if (sendBufSize < 0) {
            throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
         }

         val recvBufSize = typedProps.getIntProperty(PROP_KEY_RECV_BUF_SIZE, RECV_BUF_SIZE_DEFAULT, true)
         if (recvBufSize < 0) {
            throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
         }

         if (isDebugEnabled) {
            debug("Starting server with basic settings: host=%s, port=%d, masterThreads=%s, workerThreads=%d, " +
                  "idleTimeout=%d, tcpNoDelay=%b, sendBufSize=%d, recvBufSize=%d", host, port,
                  masterThreads, workerThreads, idleTimeout, tcpNoDelay, sendBufSize, recvBufSize)
         }

         // Register rank calculator before starting any cache so that we can capture all view changes
         cacheManager.addListener(getRankCalculatorListener)
         // Start default cache
         startDefaultCache
         startTransport(idleTimeout, tcpNoDelay, sendBufSize, recvBufSize, typedProps)
      }
   }

   def startTransport(idleTimeout: Int, tcpNoDelay: Boolean, sendBufSize: Int, recvBufSize: Int, typedProps: TypedProperties) {
      val address = new InetSocketAddress(host, port)
      transport = new NettyTransport(this, getEncoder, address, workerThreads, idleTimeout,
         threadNamePrefix, tcpNoDelay, sendBufSize, recvBufSize)
      transport.start
   }

   def start(propertiesFileName: String, cacheManager: EmbeddedCacheManager) {
      val propsObject = new TypedProperties()
      val stream = Util.loadResourceAsStream(propertiesFileName)
      propsObject.load(stream)
      start(propsObject, cacheManager)
   }

   override def stop {
      val isDebug = isDebugEnabled
      if (isDebug)
         debug("Stopping server listening in %s:%d", host, port)

      if (transport != null)
         transport.stop

      if (isDebug)
         debug("Server stopped")
   }

   def getCacheManager = cacheManager

   def getHost = host

   def getPort = port

   def startDefaultCache = cacheManager.getCache()
}
