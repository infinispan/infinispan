package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.ServerSocket;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractField;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;

/**
 * Test to verify that the listener threads are stopped.
 *
 * @author gustavonalle
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.ListenerCacheManagerStopTest")
public class ListenerCacheManagerStopTest extends AbstractInfinispanTest {

   HotRodServer hotRodServer;
   RemoteCacheManager remoteCacheManager;
   RemoteCache<Integer, String> cache;
   EmbeddedCacheManager cacheManager;

   @BeforeMethod
   public void setup() throws IOException {
      Integer port = getRandomFreePort();
      cacheManager = createCacheManager(hotRodCacheConfiguration());
      hotRodServer = HotRodTestingUtil.startHotRodServer(cacheManager, port);
      remoteCacheManager = new RemoteCacheManager(
            new ConfigurationBuilder()
                  .addServer()
                  .port(port)
                  .host(HotRodTestingUtil.host())
                  .connectionTimeout(3000)
                  .socketTimeout(3000)
                  .build()
      );
      cache = remoteCacheManager.getCache();
   }

   @AfterMethod
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hotRodServer);
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Test
   public void testThreadsAreStopped() throws Exception {
      final EventLogListener listener = new EventLogListener();
      cache.addClientListener(listener);

      final String listenerId = findListenerId(listener);
      assertListenerThreadRunning(listenerId);

      remoteCacheManager.stop();
      hotRodServer.stop();

      assertListenerThreadNotRunning(listenerId);
   }

   private String findListenerId(Object listener) {
      ClientListenerNotifier clientNotifier = extractField(remoteCacheManager, "listenerNotifier");
      return Util.toHexString(clientNotifier.findListenerId(listener), 8);

   }

   private void assertListenerThreadRunning(final String listenerId) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return isListenerThreadRunning(listenerId);
         }
      });
   }

   private void assertListenerThreadNotRunning(final String listenerId) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !isListenerThreadRunning(listenerId);
         }
      });
   }

   private boolean isListenerThreadRunning(String listenerId) {
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(false, false);
      for (ThreadInfo threadInfo : threadInfos) {
         if ((threadInfo.getThreadState().equals(Thread.State.RUNNABLE) ||
                    threadInfo.getThreadState().equals(Thread.State.BLOCKED)) &&
               threadInfo.getThreadName().contains(listenerId)) {
            return true;
         }
      }
      return false;
   }

   private Integer getRandomFreePort() throws IOException {
      try (ServerSocket socket = new ServerSocket(0)) {
         return socket.getLocalPort();
      }
   }
}


