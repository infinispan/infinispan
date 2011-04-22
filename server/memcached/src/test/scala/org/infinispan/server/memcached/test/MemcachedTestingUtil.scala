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

import java.lang.reflect.Method
import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.server.memcached.{MemcachedDecoder, MemcachedValue, MemcachedServer}
import org.infinispan.manager.EmbeddedCacheManager
import java.util.{Properties, Arrays}
import org.infinispan.server.core.Main._

/**
 * Utils for Memcached tests.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
trait MemcachedTestingUtil {
   def host = "127.0.0.1"

   def k(m: Method, prefix: String): String = prefix + m.getName

   def v(m: Method, prefix: String): String = prefix + m.getName

   def k(m: Method): String = k(m, "k-")

   def v(m: Method): String = v(m, "v-")

   def createMemcachedClient(timeout: Long, port: Int): MemcachedClient = {
      var d: DefaultConnectionFactory = new DefaultConnectionFactory {
         override def getOperationTimeout: Long = timeout
      }
      return new MemcachedClient(d, Arrays.asList(new InetSocketAddress(host, port)))
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager): MemcachedServer =
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue)

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int): MemcachedServer = {
      val server = new MemcachedServer
      server.start(getProperties(host, port), cacheManager)
      server
   }

   private def getProperties(host: String, port: Int): Properties = {
      val properties = new Properties
      properties.setProperty(PROP_KEY_HOST, host)
      properties.setProperty(PROP_KEY_PORT, port.toString)
      properties
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, cacheName: String): MemcachedServer = {
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue, cacheName)
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int, cacheName: String): MemcachedServer = {
      val server = new MemcachedServer {

         override def getDecoder: MemcachedDecoder = {
            var memcachedDecoder: MemcachedDecoder = new MemcachedDecoder(getCacheManager.getCache[String, MemcachedValue](cacheName), scheduler, transport)
            memcachedDecoder.versionGenerator = this.versionGenerator
            memcachedDecoder
         }

         override def startDefaultCache = getCacheManager.getCache(cacheName)
      }
      server.start(getProperties(host, port), cacheManager)
      server
   }
   
}

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(16211)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
