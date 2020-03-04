package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.replaceField;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.XSiteResponseImpl;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.data.DelayedMarshallingPojo;
import org.infinispan.util.ControlledTimeService;
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
      ObjectName rpcManager = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), rpcManager);
   }

   public void testEnableJmxStats() throws Exception {
      Cache<String, String> cache1 = manager(0).getCache();
      Cache cache2 = manager(1).getCache();
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      ObjectName rpcManager2 = getCacheObjectName(jmxDomain2, getDefaultCacheName() + "(repl_sync)", "RpcManager");
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
      Cache<MagicKey, Object> cache1 = manager(0).getCache();
      Cache cache2 = manager(1).getCache();
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager1 = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");

      // the previous test has reset the statistics
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationFailures"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), "N/A");

      cache1.put(new MagicKey("a1", cache1), new DelayedMarshallingPojo(50, 0));
      cache1.put(new MagicKey("a2", cache2), new DelayedMarshallingPojo(50, 0));
      assertEquals(mBeanServer.getAttribute(rpcManager1, "ReplicationCount"), (long) 2);
      assertEquals(mBeanServer.getAttribute(rpcManager1, "SuccessRatio"), "100%");
      Object avgReplTime = mBeanServer.getAttribute(rpcManager1, "AverageReplicationTime");
      assertNotEquals(avgReplTime, (long) 0);

      RpcManagerImpl rpcManager = (RpcManagerImpl) extractComponent(cache1, RpcManager.class);
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
      RpcManagerImpl rpcManager = (RpcManagerImpl) extractComponent(cache(0), RpcManager.class);
      replaceField(timeService, "timeService", rpcManager, RpcManagerImpl.class);
      Transport originalTransport = rpcManager.getTransport();

      List<XSiteResponse> responses = new ArrayList<>(3);
      List<CompletableFuture<Void>> asyncFutures = new ArrayList<>(2);
      List<CompletableFuture<Void>> syncFutures = new ArrayList<>(2);

      try {
         Transport mockTransport = mock(Transport.class);
         when(mockTransport.backupRemotely(any(XSiteBackup.class), any(XSiteReplicateCommand.class)))
               .then(invocationOnMock -> {
                  XSiteBackup arg1 = invocationOnMock.getArgument(0);
                  CompletableFuture<Void> cf = new CompletableFuture<>();
                  XSiteResponseImpl rsp = new XSiteResponseImpl(timeService, arg1);
                  if (arg1.isSync()) {
                     syncFutures.add(cf);
                  } else {
                     asyncFutures.add(cf);
                  }
                  cf.whenComplete(rsp);
                  return rsp;
               });

         rpcManager.setTransport(mockTransport);

         XSiteBackup site1 = newBackup("Site1", true);
         XSiteBackup site2 = newBackup("Site2", false);


         responses.add(rpcManager.invokeXSite(site1, mock(XSiteReplicateCommand.class)));
         responses.add(rpcManager.invokeXSite(site2, mock(XSiteReplicateCommand.class)));
         responses.add(rpcManager.invokeXSite(site2, mock(XSiteReplicateCommand.class)));
         responses.add(rpcManager.invokeXSite(site1, mock(XSiteReplicateCommand.class)));
      } finally {
         rpcManager.setTransport(originalTransport);
      }

      assertEquals(responses.size(), 4);
      assertEquals(asyncFutures.size(), 2);

      //in the end, we end up with 2 sync request and 2 async requests
      timeService.advance(10);
      asyncFutures.get(0).complete(null);
      syncFutures.get(0).complete(null);
      responses.get(0).toCompletableFuture().join();

      timeService.advance(20);
      asyncFutures.get(1).complete(null);
      syncFutures.get(1).complete(null);
      responses.get(1).toCompletableFuture().join();
      responses.get(2).toCompletableFuture().join();
      responses.get(3).toCompletableFuture().join();

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManagerName = getCacheObjectName(jmxDomain1, getDefaultCacheName() + "(repl_sync)", "RpcManager");
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "SyncXSiteCount"), (long) 2);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AsyncXSiteCount"), (long) 2);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AsyncXSiteAcksCount"), (long) 2);

      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MinimumXSiteReplicationTime"), (long) 10);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MaximumXSiteReplicationTime"), (long) 30);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AverageXSiteReplicationTime"), (long) 20);

      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MinimumAsyncXSiteReplicationTime"), (long) 10);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MaximumAsyncXSiteReplicationTime"), (long) 30);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AverageAsyncXSiteReplicationTime"), (long) 20);

      mBeanServer.invoke(rpcManagerName, "resetStatistics", new Object[0], new String[0]);

      assertEquals(mBeanServer.getAttribute(rpcManagerName, "SyncXSiteCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AsyncXSiteCount"), (long) 0);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AsyncXSiteAcksCount"), (long) 0);

      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MinimumXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MaximumXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AverageXSiteReplicationTime"), (long) -1);

      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MinimumAsyncXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "MaximumAsyncXSiteReplicationTime"), (long) -1);
      assertEquals(mBeanServer.getAttribute(rpcManagerName, "AverageAsyncXSiteReplicationTime"), (long) -1);
   }

   private static XSiteBackup newBackup(String name, boolean sync) {
      return new XSiteBackup(name, sync, Long.MAX_VALUE);
   }
}
