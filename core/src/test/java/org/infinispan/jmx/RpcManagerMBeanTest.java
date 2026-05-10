package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.MockTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.RpcManagerMBeanTest")
public class RpcManagerMBeanTest extends AbstractClusterMBeanTest {

   public RpcManagerMBeanTest() {
      super(RpcManagerMBeanTest.class.getSimpleName());
   }

   public void testJmxOperationMetadata() throws Exception {
      ObjectName rpcManager = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), rpcManager);
   }

   public void testEnableJmxStats() throws Exception {
      Cache<String, String> cache1 = manager(0).getCache();
      Cache<String, String> cache2 = manager(1).getCache();
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      ObjectName rpcManager2 = getCacheObjectName(jmxDomain2, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      assertTrue(mBeanServer.isRegistered(rpcManager1));
      assertTrue(mBeanServer.isRegistered(rpcManager2));

      Object statsEnabled = mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled");
      assertNotNull(statsEnabled);
      assertEquals(Boolean.TRUE, statsEnabled);

      assertEquals(Boolean.TRUE, mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled"));
      assertEquals(Boolean.TRUE, mBeanServer.getAttribute(rpcManager2, "StatisticsEnabled"));

      // The initial state transfer uses cache commands, so it also increases the ReplicationCount value
      long initialReplicationCount1 = (Long) mBeanServer.getAttribute(rpcManager1, "ReplicationCount");

      cache1.put("key", "value2");
      assertEquals("value2", cache2.get("key"));
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), initialReplicationCount1 + 1);
      assertEquals((long) 0, mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"));

      // now reset statistics
      mBeanServer.invoke(rpcManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals((long) 0, mBeanServer.getAttribute(rpcManager1, "ReplicationCount"));
      assertEquals((long) 0, mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"));

      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));

      cache1.put("key", "value");
      assertEquals("value", cache2.get("key"));
      assertEquals((long) -1, mBeanServer.getAttribute(rpcManager1, "ReplicationCount"));
      assertEquals((long) -1, mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"));

      // reset stats enabled parameter
      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testSuccessRatio() throws Exception {
      Cache<MagicKey, Object> cache1 = manager(0).getCache();
      Cache<MagicKey, Object> cache2 = manager(1).getCache();
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");

      // the previous test has reset the statistics
      assertEquals((long) 0, mBeanServer.getAttribute(rpcManager1, "ReplicationCount"));
      assertEquals((long) 0, mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"));
      assertEquals("N/A", mBeanServer.getAttribute(rpcManager1, "SuccessRatio"));

      RpcManagerImpl rpcManager = (RpcManagerImpl) extractComponent(cache1, RpcManager.class);
      Transport originalTransport = rpcManager.getTransport();
      try {
         MockTransport transport = new MockTransport(address(0));
         transport.init(originalTransport.getViewId(), originalTransport.getMembers());
         rpcManager.setTransport(transport);

         CompletableFuture<Object> put1 = cache1.putAsync(new MagicKey("a1", cache1), "b1");
         timeService.advance(50);
         transport.expectCommand(PutKeyValueCommand.class)
                  .singleResponse(address(2), SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
         put1.get(10, TimeUnit.SECONDS);

         CompletableFuture<Object> put2 = cache1.putAsync(new MagicKey("a2", cache2), "b2");
         timeService.advance(10);
         transport.expectCommand(PutKeyValueCommand.class)
                  .singleResponse(address(2), SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
         put2.get(10, TimeUnit.SECONDS);

         assertEquals((long) 2, mBeanServer.getAttribute(rpcManager1, "ReplicationCount"));
         assertEquals("100%", mBeanServer.getAttribute(rpcManager1, "SuccessRatio"));
         long avgReplTime = (long) mBeanServer.getAttribute(rpcManager1, "AverageReplicationTime");
         assertEquals(30, avgReplTime);

         // If cache1 is the primary owner it will be a broadcast, otherwise a unicast
         CompletableFuture<Object> put3 = cache1.putAsync(new MagicKey("a3", cache1), "b3");
         transport.expectCommand(PutKeyValueCommand.class)
                  .throwException(new RuntimeException());
         Exceptions.expectCompletionException(CacheException.class, put3);

         CompletableFuture<Object> put4 = cache1.putAsync(new MagicKey("a4", cache2), "b4");
         transport.expectCommand(PutKeyValueCommand.class)
                  .throwException(new RuntimeException());
         Exceptions.expectCompletionException(CacheException.class, put4);

         assertEquals(("50%"), mBeanServer.getAttribute(rpcManager1, "SuccessRatio"));
      } finally {
         rpcManager.setTransport(originalTransport);
      }
   }
}
