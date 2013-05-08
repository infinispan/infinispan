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
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.HeavyLoadConnectionPoolingTest", groups = "functional")
public class HeavyLoadConnectionPoolingTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private GenericKeyedObjectPool<?,?> connectionPool;

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      // make sure all operations take at least 100 msecs
      cache.getAdvancedCache().addInterceptor(new ConstantDelayTransportInterceptor(100), 0);

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.setProperty("infinispan.client.hotrod.server_list", "localhost:"+hotRodServer.getPort());
      hotrodClientConf.setProperty("timeBetweenEvictionRunsMillis", "500");
      hotrodClientConf.setProperty("minEvictableIdleTimeMillis", "100");
      hotrodClientConf.setProperty("numTestsPerEvictionRun", "10");
      hotrodClientConf.setProperty("infinispan.client.hotrod.ping_on_startup", "true");
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      connectionPool = (GenericKeyedObjectPool<?, ?>) TestingUtil.extractField(tcpConnectionFactory, "connectionPool");

      return cacheManager;
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testHeavyLoad() throws InterruptedException, ExecutionException {
      List<WorkerThread> workers = new ArrayList<WorkerThread>();

      //create 20 threads and do work with them
      for (int i =0; i < 20; i++) {
         WorkerThread workerThread = new WorkerThread(remoteCache);
         workers.add(workerThread);
         workerThread.stress();
      }
      while (connectionPool.getNumActive() <= 15) {
         Thread.sleep(10);
      }

      for (WorkerThread wt: workers) {
         wt.stop();
      }

      for (WorkerThread wt: workers) {
         wt.awaitTermination();
      }

      //now wait for the idle thread to wake up and clean them

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int numIdle = connectionPool.getNumIdle();
            int numActive = connectionPool.getNumActive();
            return numIdle == 0 && numActive == 0;
         }
      });
   }

   public static class ConstantDelayTransportInterceptor extends CommandInterceptor {

      private int millis;

      public ConstantDelayTransportInterceptor(int millis) {
         this.millis = millis;
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         Thread.sleep(millis);
         return super.handleDefault(ctx, command);
      }
   }
}
