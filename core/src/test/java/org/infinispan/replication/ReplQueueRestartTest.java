package org.infinispan.replication;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.ReplicationQueueImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "replication.ReplQueueRestartTest", groups = "unit")
public class ReplQueueRestartTest extends AbstractInfinispanTest {

   @SuppressWarnings("unchecked")
   public void testReplQueueImpl() {
      ReplicationQueueImpl rqi = new ReplicationQueueImpl();
      ScheduledFuture sf = mock(ScheduledFuture.class);
      when(sf.cancel(eq(true))).thenReturn(true);

      ScheduledExecutorService ses = mock(ScheduledExecutorService.class);
      when(ses.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
         .thenReturn(sf);

      RpcManager rpc = mock(RpcManager.class);
      CommandsFactory commandsFactory = mock(CommandsFactory.class);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_ASYNC)
            .async().useReplQueue(true);

      rqi.injectDependencies(ses, rpc, builder.build(), commandsFactory, "");

      rqi.start();

      rqi.stop();
   }

}
