package org.infinispan.client.hotrod;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put("hotrod-servers", "localhost:"+hotRodServer.getPort());
      hotrodClientConf.put("timeBetweenEvictionRunsMillis", "3000");
      hotrodClientConf.put("minEvictableIdleTimeMillis", "1000");
      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      connectionPool = (GenericKeyedObjectPool) TestingUtil.extractField(tcpConnectionFactory, "connectionPool");

      return cacheManager;
   }

   public void testHeavyLoad() throws InterruptedException {
      List<WorkerThread> workers = new ArrayList<WorkerThread>();

      //create 20 threads and do work with them
      for (int i =0; i < 20; i++) {
         WorkerThread workerThread = new WorkerThread(remoteCache);
         workers.add(workerThread);
         workerThread.stress();
      }
      while (!(connectionPool.getNumActive() > 15)) {
         Thread.sleep(10);
      }

      for (WorkerThread wt: workers) {
         wt.interrupt();
         wt.waitToFinish();
      }
      //now wait for the idle thread to wake up and clean them
      for (int i = 0; i < 50; i++) {
         System.out.println("connectionPool = " + connectionPool.getNumActive());
         if (connectionPool.getNumIdle() == 1) break;
         Thread.sleep(1000);
      }
      assertEquals(1, connectionPool.getNumIdle());
      assertEquals(0, connectionPool.getNumActive());
   }
}
