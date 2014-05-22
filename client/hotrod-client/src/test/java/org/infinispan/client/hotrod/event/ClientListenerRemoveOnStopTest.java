package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "client.hotrod.event.ClientListenerRemoveOnStopTest")
public class ClientListenerRemoveOnStopTest extends SingleHotRodServerTest {

   public void testAllListenersRemovedOnStop() {
      final EventLogListener eventListener1 = new EventLogListener();
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      rcache.addClientListener(eventListener1);
      Set<Object> listeners = rcache.getListeners();
      assertEquals(1, listeners.size());
      assertEquals(eventListener1, listeners.iterator().next());
      final EventLogListener eventListener2 = new EventLogListener();
      rcache.addClientListener(eventListener2);
      listeners = rcache.getListeners();
      assertEquals(2, listeners.size());
      assertTrue(listeners.contains(eventListener1));
      assertTrue(listeners.contains(eventListener2));
      remoteCacheManager.stop();
      listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

}
