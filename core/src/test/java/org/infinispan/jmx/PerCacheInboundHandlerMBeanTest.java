package org.infinispan.jmx;

import static org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler.MBEAN_COMPONENT_NAME;
import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.Test;

/**
 * An unit test for {@link PerCacheInboundInvocationHandler} JMX reports.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Test(groups = "functional", testName = "jmx.PerCacheInboundHandlerMBeanTest")
public class PerCacheInboundHandlerMBeanTest extends AbstractClusterMBeanTest {


   public PerCacheInboundHandlerMBeanTest() {
      super(PerCacheInboundHandlerMBeanTest.class.getSimpleName());
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(getObjectName());
   }

   public void testEnableJmxStats() throws Exception {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      PerCacheInboundInvocationHandler handler = getHandler();

      ObjectName objName = getObjectName();

      assertTrue(mBeanServer.isRegistered(objName));
      assertEquals(Boolean.TRUE, mBeanServer.getAttribute(objName, "StatisticsEnabled"));


      //check if it is collected
      handler.registerXSiteCommandReceiver(true);

      assertEquals(1, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      // now reset statistics
      mBeanServer.invoke(objName, "resetStatistics", new Object[0], new String[0]);
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      //disable it
      mBeanServer.setAttribute(objName, new Attribute("StatisticsEnabled", Boolean.FALSE));

      handler.registerXSiteCommandReceiver(true);

      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      // reset stats enabled parameter
      mBeanServer.setAttribute(objName, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   public void testStats() throws Throwable {
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      InboundInvocationHandler handler = manager(0).getGlobalComponentRegistry()
            .getComponent(InboundInvocationHandler.class);

      ObjectName objName = getObjectName();

      assertTrue(mBeanServer.isRegistered(objName));
      assertEquals(Boolean.TRUE, mBeanServer.getAttribute(objName, "StatisticsEnabled"));

      XSiteReplicateCommand command = mock(XSiteReplicateCommand.class);
      when(command.performInLocalSite(ArgumentMatchers.any(BackupReceiver.class))).thenReturn(null);
      when(command.getCacheName()).thenReturn(ByteString.fromString(cachename));

      //check if it is collected
      Reply reply = response -> {
      }; //sync reply

      //PER_SENDER to avoid span new threads
      handler.handleFromRemoteSite("another-site", command, reply, DeliverOrder.PER_SENDER);
      assertEquals(1, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      handler.handleFromRemoteSite("another-site", command, reply, DeliverOrder.PER_SENDER);
      assertEquals(2, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      //NO_OP reply identifies a asynchronous request
      handler.handleFromRemoteSite("another-site", command, Reply.NO_OP, DeliverOrder.PER_SENDER);
      assertEquals(2, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(1, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      // now reset statistics
      mBeanServer.invoke(objName, "resetStatistics", new Object[0], new String[0]);
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      handler.handleFromRemoteSite("another-site", command, Reply.NO_OP, DeliverOrder.PER_SENDER);
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(1, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      handler.handleFromRemoteSite("another-site", command, Reply.NO_OP, DeliverOrder.PER_SENDER);
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(2, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));

      // now reset statistics
      mBeanServer.invoke(objName, "resetStatistics", new Object[0], new String[0]);
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "SyncXSiteRequestsReceived"));
      assertEquals(0, (long) mBeanServer.getAttribute(objName, "AsyncXSiteRequestsReceived"));
   }


   private ObjectName getObjectName() {
      return getCacheObjectName(jmxDomain, cachename + "(repl_sync)", MBEAN_COMPONENT_NAME);
   }

   private PerCacheInboundInvocationHandler getHandler() {
      return cache(0, cachename).getAdvancedCache().getComponentRegistry().getPerCacheInboundInvocationHandler();
   }

}
