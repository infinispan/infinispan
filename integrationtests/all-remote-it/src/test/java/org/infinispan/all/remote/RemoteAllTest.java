package org.infinispan.all.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.junit.Test;

public class RemoteAllTest {

   @Test
   public void testRemoteAll() {
      RemoteCacheManager rcm = null;
      try {
         rcm = new RemoteCacheManager();
      } finally {
         if (rcm != null)
            rcm.stop();
      }
   }
}
