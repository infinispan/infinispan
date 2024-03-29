package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
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
   private ChannelFactory channelFactory;

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      // make sure all operations take at least 100 msecs
      extractInterceptorChain(cache).addInterceptor(new ConstantDelayTransportInterceptor(100), 0);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder
            .connectionPool()
               .minEvictableIdleTime(100)
               .minIdle(0)
            .addServer().host("localhost").port(hotRodServer.getPort());

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      channelFactory = TestingUtil.extractField(remoteCacheManager, "channelFactory");

      return cacheManager;
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
      hotRodServer = null;
   }

   public void testHeavyLoad() throws InterruptedException, ExecutionException {
      List<WorkerThread> workers = new ArrayList<WorkerThread>();

      //create 20 threads and do work with them
      AtomicLong opCounter = new AtomicLong(0);
      for (int i =0; i < 20; i++) {
         WorkerThread workerThread = new WorkerThread(remoteCache);
         workers.add(workerThread);
         workerThread.stress(opCounter);
      }
      while (opCounter.get() < 100) {
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
            int numIdle = channelFactory.getNumIdle();
            int numActive = channelFactory.getNumActive();
            return numIdle == 0 && numActive == 0;
         }
      });
   }

   public static class ConstantDelayTransportInterceptor extends BaseAsyncInterceptor {

      private final int millis;

      public ConstantDelayTransportInterceptor(int millis) {
         this.millis = millis;
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         Thread.sleep(millis);
         return invokeNext(ctx, command);
      }
   }
}
