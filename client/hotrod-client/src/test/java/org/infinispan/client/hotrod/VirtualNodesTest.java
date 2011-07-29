package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.SortedMap;

import static org.infinispan.test.TestingUtil.extractField;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests that verify that Hot Rod clients work as expected when virtual nodes are enabled.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "client.hotrod.VirtualNodesTest")
public class VirtualNodesTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(CacheMode.DIST_SYNC).fluent()
         .clustering().hash().numVirtualNodes(500).numOwners(1).build();
      createHotRodServers(2, config);
   }

   public void testNumVirtualNodesInClient(Method m) {
      RemoteCacheManager client = client(0);
      client.getCache().put(1, v(m));
      TcpTransportFactory tf = (TcpTransportFactory) extractField(
            client, "transportFactory");
      SortedMap<Integer, SocketAddress> positions = (SortedMap<Integer, SocketAddress>)
            extractField(tf.getConsistentHash(), "positions");
      assertEquals(1000, positions.size());
   }

}
