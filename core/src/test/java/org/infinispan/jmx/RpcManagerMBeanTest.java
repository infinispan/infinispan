package org.infinispan.jmx;

import static java.util.Collections.singletonMap;
import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.commons.time.TimeService;
import org.infinispan.distribution.MagicKey;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsBackupResponse;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
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
      ObjectName rpcManager = getCacheObjectName(jmxDomain, cachename + "(repl_sync)", "RpcManager");
      checkMBeanOperationParameterNaming(rpcManager);
   }

   public void testEnableJmxStats() throws Exception {
      Cache<String, String> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain, cachename + "(repl_sync)", "RpcManager");
      ObjectName rpcManager2 = getCacheObjectName(jmxDomain2, cachename + "(repl_sync)", "RpcManager");
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
      Cache<MagicKey, Serializable> cache1 = manager(0).getCache(cachename);
      Cache cache2 = manager(1).getCache(cachename);
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain, cachename + "(repl_sync)", "RpcManager");

      // the previous test has reset the statistics
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), "N/A");

      cache1.put(new MagicKey("a1", cache1), new SlowToSerialize("b1", 50));
      cache1.put(new MagicKey("a2", cache2), new SlowToSerialize("b2", 50));
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 2);
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
         when(transport.getAddress()).thenReturn(mockAddress1);
         // If cache1 is the primary owner it will be a broadcast, otherwise a unicast
         when(transport.invokeCommand(any(Address.class), any(ReplicableCommand.class), any(ResponseCollector.class),
                                      any(DeliverOrder.class), anyLong(), any(TimeUnit.class)))
               .thenThrow(new RuntimeException());
         when(transport.invokeCommandOnAll(any(Collection.class), any(ReplicableCommand.class), any(ResponseCollector.class),
                                           any(DeliverOrder.class), anyLong(), any(TimeUnit.class))).thenThrow(new RuntimeException());
         rpcManager.setTransport(transport);
         Exceptions.expectException(CacheException.class, () -> cache1.put(new MagicKey("a3", cache1), "b3"));
         Exceptions.expectException(CacheException.class, () -> cache1.put(new MagicKey("a4", cache2), "b4"));
         assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), ("50%"));
      } finally {
         rpcManager.setTransport(originalTransport);
      }
   }

   @Test(dependsOnMethods = "testEnableJmxStats")
   public void testXsiteStats() throws Exception {
      ControlledTimeService timeService = new ControlledTimeService();
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache(0, cachename), RpcManager.class);
      Transport originalTransport = rpcManager.getTransport();
      List<BackupResponse> responses = new ArrayList<>(3);
      try {
         Transport mockTransport = mock(Transport.class);
         when(mockTransport.backupRemotely(anyCollection(), any(XSiteReplicateCommand.class)))
               .thenReturn(mockBackupResponse(timeService));

         rpcManager.setTransport(mockTransport);

         List<XSiteBackup> remoteSites = new ArrayList<>(2);
         remoteSites.add(newBackup("Site1", true));
         remoteSites.add(newBackup("Site2", false));

         responses.add(rpcManager.invokeXSite(remoteSites, mock(XSiteReplicateCommand.class)));

         remoteSites.clear();
         remoteSites.add(newBackup("Site3", false));
         //the JGroupsTransport filters out the async request and generates an empty BackupResponse
         rpcManager.invokeXSite(remoteSites, mock(XSiteReplicateCommand.class));

         remoteSites.clear();
         remoteSites.add(newBackup("Site4", true));

         responses.add(rpcManager.invokeXSite(remoteSites, mock(XSiteReplicateCommand.class)));

      } finally {
         rpcManager.setTransport(originalTransport);
      }

      //in the end, we end up with 2 sync request and 2 async requests
      timeService.advance(10);
      responses.get(0).waitForBackupToFinish();

      timeService.advance(20);
      responses.get(1).waitForBackupToFinish();

      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName rpcManagenName = getCacheObjectName(jmxDomain, cachename + "(repl_sync)", "RpcManager");
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "SyncXSiteCount"), (long) 2);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "AsyncXSiteCount"), (long) 2);

      assertEquals(mBeanServer.getAttribute(rpcManagenName, "MinimumXSiteReplicationTime"), (long) 10);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "MaximumXSiteReplicationTime"), (long) 30);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "AverageXSiteReplicationTime"), (long) 20);

      mBeanServer.invoke(rpcManagenName, "resetStatistics", new Object[0], new String[0]);

      assertEquals(mBeanServer.getAttribute(rpcManagenName, "SyncXSiteCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "AsyncXSiteCount"), (long) 0);

      assertEquals(mBeanServer.getAttribute(rpcManagenName, "MinimumXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "MaximumXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagenName, "AverageXSiteReplicationTime"), (long) -1);
   }

   public static class SlowToSerialize implements Externalizable, ExternalPojo {
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

   private static BackupResponse mockBackupResponse(TimeService timeService) {
      XSiteBackup backup = newBackup("test", true);
      return new JGroupsBackupResponse(singletonMap(backup, CompletableFutures.completedNull()), timeService);
   }

   private static XSiteBackup newBackup(String name, boolean sync) {
      return new XSiteBackup(name, sync, Long.MAX_VALUE);
   }

}
