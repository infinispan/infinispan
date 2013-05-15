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
package org.infinispan.server.hotrod

import org.testng.annotations.Test
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.util.SslContextFactory

/**
 * Hot Rod server functional test over SSL
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSslFunctionalTest")
class HotRodSslFunctionalTest extends HotRodFunctionalTest {

   private val tccl = Thread.currentThread().getContextClassLoader
   private val keyStoreFileName = tccl.getResource("keystore.jks").getPath
   private val trustStoreFileName = tccl.getResource("truststore.jks").getPath

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = {
      val builder = new HotRodServerConfigurationBuilder
      builder.proxyHost(host).proxyPort(UniquePortThreadLocal.get.intValue).idleTimeout(0)
      builder.ssl.enable().keyStoreFileName(keyStoreFileName).keyStorePassword("secret".toCharArray).trustStoreFileName(trustStoreFileName).trustStorePassword("secret".toCharArray)
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, -1, builder)
   }

   override protected def connectClient: HotRodClient = {
      val ssl = hotRodServer.getConfiguration.ssl
      val sslContext = SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.trustStoreFileName, ssl.trustStorePassword)
      val sslEngine = SslContextFactory.getEngine(sslContext, true, false)
      new HotRodClient(host, hotRodServer.getPort, cacheName, 60, 10, sslEngine)
   }
}
