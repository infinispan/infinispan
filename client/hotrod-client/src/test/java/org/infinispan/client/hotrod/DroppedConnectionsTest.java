package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.DroppedConnectionsTest", groups = "functional")
public class DroppedConnectionsTest extends SingleCacheManagerTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache rc;
   private TcpTransportFactory transportFactory;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(getDefaultStandaloneCacheConfig(false)));
      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .connectionPool()
               .testWhileIdle(false)
               .minIdle(1)
               .maxIdle(2)
               .maxActive(2)
            .addServer().host(hotRodServer.getHost()).port(hotRodServer.getPort());

      remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
      rc = remoteCacheManager.getCache();
      transportFactory = (TcpTransportFactory) ((InternalRemoteCacheManager) remoteCacheManager).getTransportFactory();
      return cacheManager;
   }

   @AfterClass
   @Override
   protected void teardown() {
      super.teardown();
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotRodServer);
   }

   public void testClosedConnection() throws Exception {
      rc.put("k","v"); //make sure a connection is created

      GenericKeyedObjectPool keyedObjectPool = transportFactory.getConnectionPool();
      InetSocketAddress address = InetSocketAddress.createUnresolved("127.0.0.1", hotRodServer.getPort());

      assertEquals(0, keyedObjectPool.getNumActive(address));
      assertEquals(1, keyedObjectPool.getNumIdle(address));

      TcpTransport tcpConnection = (TcpTransport) keyedObjectPool.borrowObject(address);
      keyedObjectPool.returnObject(address, tcpConnection);//now we have a reference to the single connection in pool

      tcpConnection.release();

      assertEquals("v", rc.get("k"));
      assertEquals(0, keyedObjectPool.getNumActive(address));
      assertEquals(1, keyedObjectPool.getNumIdle(address));

      TcpTransport tcpConnection2 = (TcpTransport) keyedObjectPool.borrowObject(address);

      assert tcpConnection2.getId() != tcpConnection.getId();
   }

}
