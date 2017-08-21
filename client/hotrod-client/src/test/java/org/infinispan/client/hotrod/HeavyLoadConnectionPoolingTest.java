package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Condition;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

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

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .connectionPool()
               .timeBetweenEvictionRuns(500)
               .minEvictableIdleTime(100)
               .numTestsPerEvictionRun(10)
            .addServer().host("localhost").port(hotRodServer.getPort());

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
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

      Eventually.eventually(new Condition() {
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
