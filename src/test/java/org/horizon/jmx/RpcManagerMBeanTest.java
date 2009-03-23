package org.horizon.jmx;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.remoting.transport.Transport;
import org.horizon.remoting.RPCManagerImpl;
import org.horizon.remoting.RPCManager;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.testng.annotations.Test;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.RpcManagerMBeanTest")
public class RpcManagerMBeanTest extends MultipleCacheManagersTest {

   private MBeanServer mBeanServer;
   public static final String JMX_DOMAIN = RpcManagerMBeanTest.class.getSimpleName();
   private Cache cache1;
   private Cache cache2;
   private ObjectName rpcManager1;
   private ObjectName rpcManager2;


   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = TestingUtil.getGlobalConfiguration();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      CacheManager cacheManager1 = TestingUtil.createClusteredCacheManager(globalConfiguration);
      cacheManager1.start();

      GlobalConfiguration globalConfiguration2 = TestingUtil.getGlobalConfiguration();
      globalConfiguration2.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration2.setJmxDomain(JMX_DOMAIN);
      globalConfiguration2.setExposeGlobalJmxStatistics(true);
      CacheManager cacheManager2 = TestingUtil.createClusteredCacheManager(globalConfiguration2);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      defineCacheOnAllManagers("repl_sync_cache", getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC));
      cache1 = manager(0).getCache("repl_sync_cache");
      cache2 = manager(1).getCache("repl_sync_cache");
      mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      rpcManager1 = new ObjectName("RpcManagerMBeanTest:cache-name=[global],jmx-resource=RPCManager");
      rpcManager2 = new ObjectName("RpcManagerMBeanTest2:cache-name=[global],jmx-resource=RPCManager");
   }

   public void testEnableJmxStats() throws Exception {

      assert mBeanServer.isRegistered(rpcManager1);
      assert mBeanServer.isRegistered(rpcManager2);

      Object statsEnabled = mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled");
      assert statsEnabled != null;
      assert statsEnabled.equals(Boolean.FALSE);

      cache1.put("key", "value");
      assert cache2.get("key").equals("value");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("N/A");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("N/A");

      mBeanServer.invoke(rpcManager1, "setStatisticsEnabled", new Object[]{Boolean.TRUE}, new String[]{"boolean"});
      assert mBeanServer.getAttribute(rpcManager1, "StatisticsEnabled").equals(Boolean.TRUE);
      assert mBeanServer.getAttribute(rpcManager2, "StatisticsEnabled").equals(Boolean.FALSE);

      cache1.put("key", "value2");
      assert cache2.get("key").equals("value2");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("1");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals("0");
      mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("N/A");


      //now reset statistics
      mBeanServer.invoke(rpcManager1, "resetStatistics", new Object[0], new String[0]);
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("0");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals("0");
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testSuccessRatio() throws Exception {
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationCount").equals("0");
      assert mBeanServer.getAttribute(rpcManager1, "ReplicationFailures").equals("0");
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("N/A");

      cache1.put("a1", "b1");
      cache1.put("a2", "b2");
      cache1.put("a3", "b3");
      cache1.put("a4", "b4");
      assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("100%");
      RPCManagerImpl rpcManager = (RPCManagerImpl) TestingUtil.extractGlobalComponent(manager(0), RPCManager.class);
      Transport originalTransport = rpcManager.getTransport();

      try {
         Transport transport = createMock(Transport.class);
         replay(transport);
         rpcManager.setTransport(transport);
         cache1.put("a6", "b6");
         assert false : "rpc manager should had thrown an expception";
      } catch (Throwable e) {
         //expected
         assert mBeanServer.getAttribute(rpcManager1, "SuccessRatio").equals("80%");
      }
      finally {
         rpcManager.setTransport(originalTransport);
      }
   }

   @Test(dependsOnMethods = "testSuccessRatio")
   public void testAddressInformation() throws Exception {
      String cm1Address = manager(0).getAddress().toString();
      String cm2Address = "N/A";
      assert mBeanServer.getAttribute(rpcManager1, "Address").equals(cm1Address);
      assert mBeanServer.getAttribute(rpcManager2, "Address").equals(cm2Address);

      String cm1Members = mBeanServer.getAttribute(rpcManager1, "Members").toString();
      assert cm1Members.contains(cm1Address);
      assert cm1Members.contains(manager(1).getAddress().toString());
      assert mBeanServer.getAttribute(rpcManager2, "Members").equals("N/A");
   }
}
