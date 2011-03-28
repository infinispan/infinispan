package org.infinispan.client.hotrod;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ClientConnectionPoolingTest")
public class ClientConnectionPoolingTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(ClientConnectionPoolingTest.class);

   Cache c1;
   Cache c2;
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;

   RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private GenericKeyedObjectPool connectionPool;
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
      return;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      c1 = TestCacheManagerFactory.createLocalCacheManager().getCache();
      c2 = TestCacheManagerFactory.createLocalCacheManager().getCache();
      registerCacheManager(c1.getCacheManager(), c2.getCacheManager());

      hotRodServer1 = TestHelper.startHotRodServer((EmbeddedCacheManager) c1.getCacheManager());
      hotRodServer2 = TestHelper.startHotRodServer((EmbeddedCacheManager) c2.getCacheManager());

      String servers = TestHelper.getServersString(hotRodServer1, hotRodServer2);
      Properties hotrodClientConf = new Properties();
      hotrodClientConf.put(ConfigurationProperties.SERVER_LIST, servers);
      hotrodClientConf.put("maxActive", 2);
      hotrodClientConf.put("maxTotal", 8);
      hotrodClientConf.put("maxIdle", 6);
      hotrodClientConf.put("whenExhaustedAction", 1);
      hotrodClientConf.put("testOnBorrow", "false");
      hotrodClientConf.put("testOnReturn", "false");
      hotrodClientConf.put("timeBetweenEvictionRunsMillis", "-2");
      hotrodClientConf.put("minEvictableIdleTimeMillis", "7");
      hotrodClientConf.put("testWhileIdle", "true");
      hotrodClientConf.put("minIdle", "-5");
      hotrodClientConf.put("lifo", "true");
      hotrodClientConf.put("infinispan.client.hotrod.ping_on_startup", "false");

      remoteCacheManager = new RemoteCacheManager(hotrodClientConf);
      remoteCache = remoteCacheManager.getCache();

      TcpTransportFactory tcpConnectionFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");
      connectionPool = (GenericKeyedObjectPool) TestingUtil.extractField(tcpConnectionFactory, "connectionPool");
      workerThread1 = new WorkerThread(remoteCache);
      workerThread2 = new WorkerThread(remoteCache);
      workerThread3 = new WorkerThread(remoteCache);
      workerThread4 = new WorkerThread(remoteCache);
      workerThread5 = new WorkerThread(remoteCache);
      workerThread6 = new WorkerThread(remoteCache);

      hrServ1Addr = new InetSocketAddress("localhost", hotRodServer1.getPort());
      hrServ2Addr = new InetSocketAddress("localhost", hotRodServer2.getPort());
   }

   @AfterTest(alwaysRun = true)
   public void tearDown() {
      hotRodServer1.stop();
      hotRodServer2.stop();
      workerThread1.stopThread();
      workerThread2.stopThread();
      workerThread3.stopThread();
      workerThread4.stopThread();
      workerThread5.stopThread();
      workerThread6.stopThread();
      remoteCacheManager.stop();
   }

   public void testPropsCorrectlySet() {
      assertEquals(2, connectionPool.getMaxActive());
      assertEquals(8, connectionPool.getMaxTotal());
      assertEquals(6, connectionPool.getMaxIdle());
      assertEquals(1, connectionPool.getWhenExhaustedAction());
      assertEquals(false, connectionPool.getTestOnBorrow());
      assertEquals(false, connectionPool.getTestOnReturn());
      assertEquals(-2, connectionPool.getTimeBetweenEvictionRunsMillis());
      assertEquals(7, connectionPool.getMinEvictableIdleTimeMillis());
      assertEquals(true, connectionPool.getTestWhileIdle());
      assertEquals(-5, connectionPool.getMinIdle());
      assertEquals(true, connectionPool.getLifo());
   }

   public void testMaxActiveReached() throws Exception {
      workerThread1.put("k1", "v1");
      workerThread1.put("k2", "v2");


      assertEquals(1, c1.size());
      assertEquals(1, c2.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals(1, c1.size());
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals(1, c2.size());

      assertEquals(1, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(1, connectionPool.getNumIdle(hrServ2Addr));

      DelayTransportInterceptor dt1 = new DelayTransportInterceptor(true);
      DelayTransportInterceptor dt2 = new DelayTransportInterceptor(true);
      c1.getAdvancedCache().addInterceptor(dt1, 0);
      c2.getAdvancedCache().addInterceptor(dt2, 0);
      log.info("Interceptors added");

      workerThread1.putAsync("k3", "v3");
      workerThread2.putAsync("k4", "v4");
      log.info("Async calls for k3 and k4 is done.");
      for (int i = 0; i < 10; i++) {
         log.trace("Active for server " + hrServ1Addr + " are:" + connectionPool.getNumActive(hrServ1Addr));
         log.trace("Active for server " + hrServ2Addr + " are:" + connectionPool.getNumActive(hrServ2Addr));
         if (connectionPool.getNumActive(hrServ1Addr) == 1 && connectionPool.getNumActive(hrServ2Addr) == 1) break;
         Thread.sleep(1000);
      }
      log.info("Connection pool is " + connectionPool);
      assertEquals(2, connectionPool.getNumActive(hrServ1Addr));
      assertEquals(1, connectionPool.getNumActive(hrServ2Addr));
      assertEquals(0, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(0, connectionPool.getNumIdle(hrServ2Addr));

      workerThread3.putAsync("k5", "v5");
      workerThread4.putAsync("k6", "v6");
      for (int i = 0; i < 10; i++) {
         log.trace("Active for server " + hrServ1Addr + " are:" + connectionPool.getNumActive(hrServ1Addr));
         log.trace("Active for server " + hrServ2Addr + " are:" + connectionPool.getNumActive(hrServ2Addr));
         if (connectionPool.getNumActive(hrServ1Addr) == 2 && connectionPool.getNumActive(hrServ2Addr) == 2) break;
         Thread.sleep(1000);
      }
      assertEquals(0, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(0, connectionPool.getNumIdle(hrServ2Addr));

      workerThread5.putAsync("k7", "v7");
      workerThread6.putAsync("k8", "v8");
      assertEquals(2, connectionPool.getNumActive(hrServ1Addr));
      assertEquals(2, connectionPool.getNumActive(hrServ2Addr));
      assertEquals(0, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(0, connectionPool.getNumIdle(hrServ2Addr));

      //now allow
      dt1.allow();
      dt2.allow();

      assertExistKeyValue("k3", "v3");
      assertExistKeyValue("k4", "v4");
      assertExistKeyValue("k5", "v5");
      assertExistKeyValue("k6", "v6");
      assertExistKeyValue("k7", "v7");
      assertExistKeyValue("k8", "v8");

      assertEquals(1, connectionPool.getNumActive(hrServ1Addr));
      assertEquals(0, connectionPool.getNumActive(hrServ2Addr));

      assertEquals(1, connectionPool.getNumIdle(hrServ1Addr));
      assertEquals(2, connectionPool.getNumIdle(hrServ2Addr));


      assertEquals(1, connectionPool.getNumIdle(hrServ1Addr));
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
