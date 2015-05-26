package org.infinispan.jmx;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.RpcManagerMBeanTest")
public class RpcManagerMBeanTest extends MultipleCacheManagersTest {
   private final String cachename = "repl_sync_cache";
   public static final String JMX_DOMAIN = RpcManagerMBeanTest.class.getSimpleName();
   public static final String JMX_DOMAIN2 = JMX_DOMAIN + "2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.globalJmxStatistics()
            .enable()
            .allowDuplicateDomains(true)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(gcb1, defaultConfig,
            new TransportFlags(), true);
      cacheManager1.start();

      GlobalConfigurationBuilder gcb2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb2.globalJmxStatistics()
            .enable()
            .allowDuplicateDomains(true)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());
      CacheContainer cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(gcb2, defaultConfig,
            new TransportFlags(), true);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).jmxStatistics().enable();
      defineConfigurationOnAllManagers(cachename, cb);
      waitForClusterToForm(cachename);
   }

   public void testJmxOperationMetadata() throws Exception {
      ObjectName rpcManager = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "RpcManager");
      checkMBeanOperationParameterNaming(rpcManager);
   }

   public void testEnableJmxStats() throws Exception {
      Cache<String, String> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "RpcManager");
      ObjectName rpcManager2 = getCacheObjectName(JMX_DOMAIN2, cachename + "(repl_sync)", "RpcManager");
      assert mBeanServer.isRegistered(rpcManager1);
      assert mBeanServer.isRegistered(rpcManager2);

      Object statsEnabled = mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled");
      assert statsEnabled != null;
      assertEquals(statsEnabled, Boolean.TRUE);

      assertEquals(mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled"), Boolean.TRUE);
      assertEquals(mBeanServer.getAttribute(rpcManager2, "StatisticsEnabled"), Boolean.TRUE);

      // The initial state transfer uses cache commands, so it also increases the ReplicationCount value
      long initialReplicationCount1 = (Long) mBeanServer.getAttribute(rpcManager1, "ReplicationCount");

      cache1.put("key", "value2");
      assertEquals(cache2.get("key"), "value2");
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), initialReplicationCount1 + 1);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) 0);

      // now reset statistics
      mBeanServer.invoke(rpcManager1, "resetStatistics", new Object[0], new String[0]);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) 0);

      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));

      cache1.put("key", "value");
      assertEquals(cache2.get("key"), "value");
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testSuccessRatio() throws Exception {
      Cache<String, Serializable> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(JMX_DOMAIN, cachename + "(repl_sync)", "RpcManager");

      // the previous test has reset the statistics
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), "N/A");

      cache1.put("a1", new SlowToSerialize("b1", 50));
      cache1.put("a2", new SlowToSerialize("b2", 50));
      cache1.put("a3", new SlowToSerialize("b3", 50));
      cache1.put("a4", new SlowToSerialize("b4", 50));
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 4);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), "100%");
      Object avgReplTime = mBeanServer.getAttribute(rpcManager1, "AverageReplicationTime");
      assertNotEquals(avgReplTime, (long) 0);

      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = rpcManager.getTransport();

      try {
         Address mockAddress1 = mock(Address.class);
         Address mockAddress2 = mock(Address.class);
         List<Address> memberList = new ArrayList<>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);
         Transport transport = mock(Transport.class);
         when(transport.getMembers()).thenReturn(memberList);
         when(transport.getAddress()).thenReturn(null);
         when(transport.invokeRemotelyAsync(anyCollectionOf(Address.class), any(ReplicableCommand.class), any(ResponseMode.class),
               anyLong(), any(ResponseFilter.class), any(DeliverOrder.class), anyBoolean())).thenThrow(new RuntimeException());
         rpcManager.setTransport(transport);
         cache1.put("a5", "b5");
         assert false : "rpc manager should have thrown an exception";
      } catch (Throwable expected) {
         //expected
         assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), ("80%"));
      }
      finally {
         rpcManager.setTransport(originalTransport);
      }
   }

   public static class SlowToSerialize implements Externalizable {
      String val;
      transient long delay;

      public SlowToSerialize() {
      }

      private SlowToSerialize(String val, long delay) {
         this.val = val;
         this.delay = delay;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         out.writeObject(val);
         TestingUtil.sleepThread(delay);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         val = (String) in.readObject();
         TestingUtil.sleepThread(delay);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         SlowToSerialize that = (SlowToSerialize) o;

         if (val != null ? !val.equals(that.val) : that.val != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return val != null ? val.hashCode() : 0;
      }
   }

}
