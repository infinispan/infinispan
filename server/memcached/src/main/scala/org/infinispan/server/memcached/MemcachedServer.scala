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
package org.infinispan.server.memcached

import org.infinispan.server.core.AbstractProtocolServer
import java.util.concurrent.Executors
import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration
import org.infinispan.AdvancedCache
import org.infinispan.configuration.cache.ConfigurationBuilder

/**
 * Memcached server defining its decoder/encoder settings. In fact, Memcached does not use an encoder since there's
 * no really common headers between protocol operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class MemcachedServer extends AbstractProtocolServer("Memcached") {
   type SuitableConfiguration = MemcachedServerConfiguration

   protected lazy val scheduler = Executors.newScheduledThreadPool(1)
   private var memcachedCache: AdvancedCache[String, Array[Byte]] = _

   override def start(configuration: MemcachedServerConfiguration, cacheManager: EmbeddedCacheManager) {
      // Define the Memcached cache as clone of the default one
      cacheManager.defineConfiguration(configuration.cache,
         new ConfigurationBuilder().read(cacheManager.getDefaultCacheConfiguration).build())
      memcachedCache = cacheManager.getCache[String, Array[Byte]](configuration.cache).getAdvancedCache
      super.start(configuration, cacheManager)
   }

   override def getEncoder = null

   override def getDecoder: MemcachedDecoder = {
      val dec = new MemcachedDecoder(memcachedCache, scheduler, transport)
      dec.versionGenerator = this.versionGenerator
      dec
   }

   override def stop {
      super.stop
      scheduler.shutdown()
   }
}
