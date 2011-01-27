package org.infinispan.replication;

import org.easymock.EasyMock;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.verify;

@Test(testName = "replication.ReplQueueRestartTest", groups = "unit")
public class ReplQueueRestartTest extends AbstractInfinispanTest {

   @SuppressWarnings("unchecked")
   public void testReplQueueImpl() {
      ReplicationQueueImpl rqi = new ReplicationQueueImpl();
      ScheduledFuture sf = createMock(ScheduledFuture.class);
      expect(sf.cancel(eq(true))).andReturn(true).once();

      ScheduledExecutorService ses = createMock(ScheduledExecutorService.class);
      expect(ses.scheduleWithFixedDelay(EasyMock.<Runnable>anyObject(), anyLong(), anyLong(), EasyMock.<TimeUnit>anyObject()))
         .andReturn(sf).once();

      RpcManager rpc = createNiceMock(RpcManager.class);
      CommandsFactory commandsFactory = createNiceMock(CommandsFactory.class);
      Configuration c = new Configuration();
      c.setUseReplQueue(true);
      replay(ses, rpc, commandsFactory,sf);

      rqi.injectDependencies(ses, rpc, c, commandsFactory);

      rqi.start();

      rqi.stop();

      verify(ses, rpc, commandsFactory, sf);
   }
}
