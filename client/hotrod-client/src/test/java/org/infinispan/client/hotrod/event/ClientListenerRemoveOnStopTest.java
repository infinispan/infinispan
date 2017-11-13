package org.infinispan.client.hotrod.event;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerRemoveOnStopTest")
public class ClientListenerRemoveOnStopTest extends SingleHotRodServerTest {

   public void testAllListenersRemovedOnStop() {
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      final EventLogListener<Integer> eventListener1 = new EventLogListener<>(rcache);
      rcache.addClientListener(eventListener1);
      Set<Object> listeners = rcache.getListeners();
      assertEquals(1, listeners.size());
      assertEquals(eventListener1, listeners.iterator().next());
      final EventLogListener<Integer> eventListener2 = new EventLogListener<>(rcache);
      rcache.addClientListener(eventListener2);
      listeners = rcache.getListeners();
      assertEquals(2, listeners.size());
      assertTrue(listeners.contains(eventListener1));
      assertTrue(listeners.contains(eventListener2));
      remoteCacheManager.stop();
      listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

   public void testRemoveListenerAfterStopAndRestart() {
      remoteCacheManager.start();
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      final EventLogListener<Integer> eventListener1 = new EventLogListener<>(rcache);
      rcache.addClientListener(eventListener1);
      Set<Object> listeners = rcache.getListeners();
      assertEquals(1, listeners.size());
      assertTrue(listeners.contains(eventListener1));
      int port = this.hotrodServer.getPort();
      HotRodClientTestingUtil.killServers(this.hotrodServer);
      listeners = rcache.getListeners();
      // The listener is removed as soon as the channel is closed
      assertEquals(0, listeners.size());
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(this.cacheManager, port, new HotRodServerConfigurationBuilder());
      rcache.removeClientListener(eventListener1);
      listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

}
