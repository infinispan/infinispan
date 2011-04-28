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

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.HeavyLoadConnectionPoolingTest", groups = "functional")
public class HeavyLoadConnectionPoolingTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private GenericKeyedObjectPool connectionPool;

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cache = cacheManager.getCache();

      // make sure all operations take at least 100 msecs
      cache.getAdvancedCache().addInterceptor(new ConstantDelayTransportInterceptor(100), 0);

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("infinispan.client.hotrod.server_list", "localhost:"+hotRodServer.getPort());
      hotrodClientConf.put("timeBetweenEvictionRunsMillis", "500");
      hotrodClientConf.put("minEvictableIdleTimeMillis", "100");
      hotrodClientConf.put("numTestsPerEvictionRun", "10");
      hotrodClientConf.put("infinispan.client.hotrod.ping_on_startup", "true");
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      connectionPool = (GenericKeyedObjectPool) TestingUtil.extractField(tcpConnectionFactory, "connectionPool");

      return cacheManager;
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      remoteCacheManager.stop();
      hotRodServer.stop();
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
      // the eviction thread cleans up at most 10 connections at a time, so we need to let it run at least 2 times
      Thread.sleep(500 * 3);

      assertEquals(1, connectionPool.getNumIdle());
      assertEquals(0, connectionPool.getNumActive());
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
