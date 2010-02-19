package org.infinispan.jmx;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import static org.easymock.EasyMock.expect;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TODO: For some reason, if you add to any of the methods below 'assert false;'
 * Eclipse 3.5 and org.testng.eclipse_5.9.0.4.jar combination will indicate that 
 * the test passes correctly. Command line mvn execution does show the failure.
 * Need to show this to Max either in the office or via a screencast to see how 
 * to debug it.
 * 
 * More information: Seems to be a problem with enabling java assertion. If no -ea is
 * passed, the command line does show '-ea' but no assertions are checked. If -ear is 
 * explicitly passed, you see '-ea -ea' in the command line and then assertions are 
 * enabled.
 * 
 * A workaround in Eclipse is to add -ea to the default VM parameters used.
 * 
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.RpcManagerMBeanTest")
public class RpcManagerMBeanTest extends MultipleCacheManagersTest {
   private final String cachename = "repl_sync_cache";
   public static final String JMX_DOMAIN = RpcManagerMBeanTest.class.getSimpleName();  

   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      CacheManager cacheManager1 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      cacheManager1.start();

      GlobalConfiguration globalConfiguration2 = GlobalConfiguration.getClusteredDefault();
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration2.setJmxDomain(JMX_DOMAIN);
      globalConfiguration2.setAllowDuplicateDomains(true);
      CacheManager cacheManager2 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration2);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      config.setExposeJmxStatistics(true);
      defineConfigurationOnAllManagers(cachename, config);
   }

   public void testEnableJmxStats() throws Exception {
      Cache cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = new ObjectName("RpcManagerMBeanTest:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      ObjectName rpcManager2 = new ObjectName("RpcManagerMBeanTest2:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      assert mBeanServer.isRegistered(rpcManager1);
      assert mBeanServer.isRegistered(rpcManager2);

      Object statsEnabled = mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled");
      assert statsEnabled != null;
      assert statsEnabled.equals(Boolean.TRUE);

      assert mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled").equals(Boolean.TRUE);
      assert mBeanServer.getAttribute(rpcManager2, "StatisticsEnabled").equals(Boolean.TRUE);

      cache1.put("key", "value2");
      assert cache2.get("key").equals("value2");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 1) : "Expected 1, was " + mBeanServer.getAttribute(rpcManager1, "ReplicationCount");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);
      mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) -1);

      // now resume statistics
      mBeanServer.invoke(rpcManager1, "resetStatistics", new Object[0], new String[0]);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);

      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.FALSE));

      cache1.put("key", "value");
      assert cache2.get("key").equals("value");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) -1);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) -1);

      // reset stats enabled parameter
      mBeanServer.setAttribute(rpcManager1, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testSuccessRatio() throws Exception {
      Cache cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = new ObjectName("RpcManagerMBeanTest:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      ObjectName rpcManager2 = new ObjectName("RpcManagerMBeanTest2:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals((long) 0);
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("N/A");

      cache1.put("a1", new SlowToSerialize("b1", 50));
      cache1.put("a2", new SlowToSerialize("b2", 50));
      cache1.put("a3", new SlowToSerialize("b3", 50));
      cache1.put("a4", new SlowToSerialize("b4", 50));
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals((long) 4);
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("100%");
      Object avgReplTime = mBeanServer.getAttribute(rpcManager1, "AverageReplicationTime");
      assert !avgReplTime.equals((long) 0) : "Expected !0, was " + avgReplTime;

      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = rpcManager.getTransport();

      try {
         Address mockAddress1 = createNiceMock(Address.class);
         Address mockAddress2 = createNiceMock(Address.class);
         List<Address> memberList = new ArrayList<Address>(2);
         memberList.add(mockAddress1);
         memberList.add(mockAddress2);
         Transport transport = createMock(Transport.class);
         expect(transport.getMembers()).andReturn(memberList).anyTimes();
         expect(transport.getAddress()).andReturn(null).anyTimes();
         expect(transport.invokeRemotely(EasyMock.<Collection<Address>>anyObject(), EasyMock.<ReplicableCommand>anyObject(),
                                                  EasyMock.<ResponseMode>anyObject(), anyLong(), anyBoolean(), EasyMock.<ResponseFilter>anyObject(),
                                                  anyBoolean())).andThrow(new RuntimeException()).anyTimes();
         replay(transport);
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

   private static class SlowToSerialize implements Externalizable {
      String val;
      transient long delay;

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

   @Test(dependsOnMethods = "testSuccessRatio")
   public void testAddressInformation() throws Exception {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = new ObjectName("RpcManagerMBeanTest:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      ObjectName rpcManager2 = new ObjectName("RpcManagerMBeanTest2:cache-name=" + cachename + "(repl_sync),jmx-resource=RpcManager");
      String cm1Address = manager(0).getAddress().toString();
      String cm2Address = manager(1).getAddress().toString();
      assert mBeanServer.getAttribute(rpcManager1, "Address").equals(cm1Address);
      assert mBeanServer.getAttribute(rpcManager2, "Address").equals(cm2Address);

      String cm1Members = mBeanServer.getAttribute(rpcManager1, "Members").toString();
      assert cm1Members.contains(cm1Address);
      assert cm1Members.contains(cm2Address);
      assert !mBeanServer.getAttribute(rpcManager2, "Members").equals("N/A");
   }
}
