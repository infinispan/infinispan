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
package org.infinispan.server.memcached.test

import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.server.memcached.{MemcachedDecoder, MemcachedServer}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder

/**
 * Utils for Memcached tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object MemcachedTestingUtil {

   def host = "127.0.0.1"

   def createMemcachedClient(timeout: Long, port: Int): MemcachedClient = {
      val d: DefaultConnectionFactory = new DefaultConnectionFactory {
         override def getOperationTimeout: Long = timeout
      }
      new MemcachedClient(d, util.Arrays.asList(new InetSocketAddress(host, port)))
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager): MemcachedServer =
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue)

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int): MemcachedServer = {
      val server = new MemcachedServer
      server.start(new MemcachedServerConfigurationBuilder().host(host).port(port).build(), cacheManager)
      server
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, cacheName: String): MemcachedServer = {
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue, cacheName)
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int, cacheName: String): MemcachedServer = {
      val server = new MemcachedServer {

         override def getDecoder: MemcachedDecoder = {
            val memcachedDecoder = new MemcachedDecoder(
               getCacheManager.getCache[String, Array[Byte]](cacheName).getAdvancedCache, scheduler, transport)
            memcachedDecoder.versionGenerator = this.versionGenerator
            memcachedDecoder
         }

         override def startDefaultCache = getCacheManager.getCache(cacheName)
      }
      server.start(new MemcachedServerConfigurationBuilder().host(host).port(port).build(), cacheManager)
      server
   }

   def killClient(client: MemcachedClient) {
      try {
         if (client != null) client.shutdown()
      }
      catch {
         case t: Throwable => {
            error("Error stopping client", t)
         }
      }
   }

}

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(16211)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
