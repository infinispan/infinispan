/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.net.SocketTimeoutException;

import javax.net.ssl.SSLException;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Adrian Brock
 * @author Tristan Tarrant
 * @since 5.3
 */
@Test(testName = "client.hotrod.SslTest", groups = "functional")
public class SslTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(SslTest.class);

   RemoteCache<String, String> defaultRemote;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      cacheManager.getCache();

      return cacheManager;
   }

   private void initServerAndClient(boolean sslServer, boolean sslClient) {
      hotrodServer = new HotRodServer();
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();

      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      String keyStoreFileName = tccl.getResource("keystore.jks").getPath();
      String trustStoreFileName = tccl.getResource("truststore.jks").getPath();
      serverBuilder.ssl()
         .enabled(sslServer)
         .keyStoreFileName(keyStoreFileName)
         .keyStorePassword("secret".toCharArray())
         .trustStoreFileName(trustStoreFileName)
         .trustStorePassword("secret".toCharArray());
      hotrodServer.start(serverBuilder.build(), cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder
         .addServer()
            .host("127.0.0.1")
            .port(hotrodServer.getPort())
            .socketTimeout(3000)
         .connectionPool()
            .maxActive(1)
         .ssl()
            .enabled(sslClient)
            .keyStoreFileName(keyStoreFileName)
            .keyStorePassword("secret".toCharArray())
            .trustStoreFileName(trustStoreFileName)
            .trustStorePassword("secret".toCharArray())
          .connectionPool()
             .timeBetweenEvictionRuns(2000);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      defaultRemote = remoteCacheManager.getCache();
   }

   @AfterMethod(alwaysRun=true)
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   public void testSSLServerSSLClient() throws Exception {
      initServerAndClient(true, true);
      defaultRemote.put("k","v");
      assert defaultRemote.get("k").equals("v");
   }

   public void testSSLServerPlainClient() throws Exception {
      try {
         // The server discards data it doesn't understand so we use a client timeout to determine that things don't work
         initServerAndClient(true, false);
         fail("Expecting a SocketTimeoutException");
      } catch (TransportException e) {
         assertTrue(e.getCause() instanceof SocketTimeoutException);
      }
   }

   public void testPlainServerSSLClient() throws Exception {
      try {
         initServerAndClient(false, true);
         fail("Expecting a SSLException");
      } catch (TransportException e) {
         assertTrue(e.getCause() instanceof SSLException);
      }
   }
}
