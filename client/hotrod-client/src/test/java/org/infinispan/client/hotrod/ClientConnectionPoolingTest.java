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
package org.infinispan.client.hotrod;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ClientConnectionPoolingTest", groups="functional")
public class ClientConnectionPoolingTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(ClientConnectionPoolingTest.class);

   Cache<String, String> c1;
   Cache<String, String> c2;
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;

   RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private GenericKeyedObjectPool<?, ?> connectionPool;
   private InetSocketAddress hrServ1Addr;
   private InetSocketAddress hrServ2Addr;


   private WorkerThread workerThread1;
   private WorkerThread workerThread2;
   private WorkerThread workerThread3;
   private WorkerThread workerThread4;
   private WorkerThread workerThread5;
   private WorkerThread workerThread6;


   @Override
   protected void assertSupportedConfig() {
      // No-op
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // The caches are not configured to form a cluster
      // so the client will have to use round-robin for balancing.
      // This means requests will alternate between server 1 and server 2.
      c1 = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()).getCache();
      c2 = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()).getCache();
      registerCacheManager(c1.getCacheManager(), c2.getCacheManager());

      hotRodServer1 = TestHelper.startHotRodServer(c1.getCacheManager());
      hotRodServer2 = TestHelper.startHotRodServer(c2.getCacheManager());

      String servers = TestHelper.getServersString(hotRodServer1, hotRodServer2);
      Properties hotrodClientConf = new Properties();
      hotrodClientConf.setProperty(ConfigurationProperties.SERVER_LIST, servers);
      hotrodClientConf.setProperty("maxActive", "2");
      hotrodClientConf.setProperty("maxTotal", "8");
      hotrodClientConf.setProperty("maxIdle", "6");
      hotrodClientConf.setProperty("whenExhaustedAction", "1");
      hotrodClientConf.setProperty("testOnBorrow", "false");
      hotrodClientConf.setProperty("testOnReturn", "false");
      hotrodClientConf.setProperty("timeBetweenEvictionRunsMillis", "-2");
      hotrodClientConf.setProperty("minEvictableIdleTimeMillis", "7");
      hotrodClientConf.setProperty("testWhileIdle", "true");
      hotrodClientConf.setProperty("minIdle", "-5");
      hotrodClientConf.setProperty("lifo", "true");
      hotrodClientConf.setProperty("infinispan.client.hotrod.ping_on_startup", "false");

      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) extractField(remoteCacheManager, "transportFactory");
      connectionPool = (GenericKeyedObjectPool<?, ?>) extractField(tcpConnectionFactory, "connectionPool");
      workerThread1 = new WorkerThread(remoteCache);
      workerThread2 = new WorkerThread(remoteCache);
      workerThread3 = new WorkerThread(remoteCache);
      workerThread4 = new WorkerThread(remoteCache);
      workerThread5 = new WorkerThread(remoteCache);
      workerThread6 = new WorkerThread(remoteCache);

      hrServ1Addr = new InetSocketAddress("localhost", hotRodServer1.getPort());
      hrServ2Addr = new InetSocketAddress("localhost", hotRodServer2.getPort());
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws ExecutionException, InterruptedException {
      killServers(hotRodServer1, hotRodServer2);

      workerThread1.stop();
      workerThread2.stop();
      workerThread3.stop();
      workerThread4.stop();
      workerThread5.stop();
      workerThread6.stop();

      workerThread1.awaitTermination();
      workerThread2.awaitTermination();
      workerThread3.awaitTermination();
      workerThread4.awaitTermination();
      workerThread5.awaitTermination();
      workerThread6.awaitTermination();

      killRemoteCacheManager(remoteCacheManager);
   }

   @Test
   public void testPropsCorrectlySet() {
      assertEquals(2, connectionPool.getMaxActive());
      assertEquals(8, connectionPool.getMaxTotal());
      assertEquals(6, connectionPool.getMaxIdle());
      assertEquals(1, connectionPool.getWhenExhaustedAction());
      assertFalse(connectionPool.getTestOnBorrow());
      assertFalse(connectionPool.getTestOnReturn());
      assertEquals(-2, connectionPool.getTimeBetweenEvictionRunsMillis());
      assertEquals(7, connectionPool.getMinEvictableIdleTimeMillis());
      assertTrue(connectionPool.getTestWhileIdle());
      assertEquals(-5, connectionPool.getMinIdle());
      assertTrue(connectionPool.getLifo());
   }

   public void testMaxActiveReached() throws Exception {
      workerThread1.put("k1", "v1");
      workerThread1.put("k2", "v2");

      // verify that each cache got a request
      assertEquals(1, c1.size());
      assertEquals(1, c2.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals(1, c1.size());
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals(1, c2.size());

      // there should be no active connections to any server
      assertEquals(0, connectionPool.getNumActive(hrServ1Addr));
      assertEquals(0, connectionPool.getNumActive(hrServ2Addr));
      assertEquals(1, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(1, connectionPool.getNumIdle(hrServ2Addr));

      // install an interceptor that will block all requests on the server until the allow() call
      DelayTransportInterceptor dt1 = new DelayTransportInterceptor(true);
      DelayTransportInterceptor dt2 = new DelayTransportInterceptor(true);
      c1.getAdvancedCache().addInterceptor(dt1, 0);
      c2.getAdvancedCache().addInterceptor(dt2, 0);
      log.info("Cache operations blocked");

      try {
         // start one operation on each server, using the existing connections
         workerThread1.putAsync("k3", "v3");
         workerThread2.putAsync("k4", "v4");
         log.info("Async calls for k3 and k4 is done.");

         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return 1 == connectionPool.getNumActive(hrServ1Addr) &&
               1 == connectionPool.getNumActive(hrServ2Addr) &&
               0 == connectionPool.getNumIdle(hrServ1Addr) &&
               0 == connectionPool.getNumIdle(hrServ2Addr);
            }
         });


         // another operation for each server, creating new connections
         workerThread3.putAsync("k5", "v5");
         workerThread4.putAsync("k6", "v6");
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return 2 == connectionPool.getNumActive(hrServ1Addr) &&
                     2 == connectionPool.getNumActive(hrServ2Addr) &&
                     0 == connectionPool.getNumIdle(hrServ1Addr) &&
                     0 == connectionPool.getNumIdle(hrServ2Addr);
            }
         });

         // we've reached the connection pool limit, the new operations will block
         // until a connection is released
         workerThread5.putAsync("k7", "v7");
         workerThread6.putAsync("k8", "v8");
         Thread.sleep(2000); //sleep a bit longer to make sure the async threads do their job
         assertEquals(2, connectionPool.getNumActive(hrServ1Addr));
         assertEquals(2, connectionPool.getNumActive(hrServ2Addr));
         assertEquals(0, connectionPool.getNumIdle(hrServ1Addr));
         assertEquals(0, connectionPool.getNumIdle(hrServ2Addr));
      }
      catch (Exception e) {
         log.error(e);
      } finally {
         //now allow
         dt1.allow();
         dt2.allow();
      }

      // give the servers some time to process the operations
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return connectionPool.getNumActive() == 0;
         }
      }, 1000);

      assertExistKeyValue("k3", "v3");
      assertExistKeyValue("k4", "v4");
      assertExistKeyValue("k5", "v5");
      assertExistKeyValue("k6", "v6");
      assertExistKeyValue("k7", "v7");
      assertExistKeyValue("k8", "v8");

      // all the connections have been released to the pool, but haven't been closed
      assertEquals(0, connectionPool.getNumActive(hrServ1Addr));
      assertEquals(0, connectionPool.getNumActive(hrServ2Addr));
      assertEquals(2, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(2, connectionPool.getNumIdle(hrServ2Addr));
   }

   private void assertExistKeyValue(String key, String value) throws InterruptedException {
      boolean exists = false;
      for (int i = 0; i < 10; i++) {
         exists = value.equals(remoteCache.get(key)) || value.equals(remoteCache.get(key));
         if (exists)  break;
         Thread.sleep(1000);
      }
      assertEquals("key value not found: (" + key + ", " + value + ")", true, exists);
   }

   public static class DelayTransportInterceptor extends CommandInterceptor {

      private final ReentrantLock lock = new ReentrantLock();

      public DelayTransportInterceptor(boolean lock) {
         if (lock)
            block();
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         log.trace("Acquiring lock. " + lockInfo());
         lock.lock();
         try {
            return super.handleDefault(ctx, command);
         } finally {
            log.trace("Done operation, releasing lock" + lockInfo());
            lock.unlock();
         }
      }

      private String lockInfo() {
         return " Is locked? " + lock.isLocked() + ". Lock held by me? " + lock.isHeldByCurrentThread();
      }

      public void block() {
         log.trace("block. " + lockInfo());
         lock.lock();
      }

      public void allow() {
         log.trace("allow." + lockInfo());
         lock.unlock();
      }
   }
}
