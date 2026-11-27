package org.infinispan.remoting.inboundhandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "remoting.inboundhandler.DefaultTopologyRunnableTest")
public class DefaultTopologyRunnableTest extends AbstractInfinispanTest {

   public void testWaitingCommandIsNotInvokedWhenCacheStopsWhileWaiting() {
      BasePerCacheInboundInvocationHandler handler = mock(BasePerCacheInboundInvocationHandler.class);
      StateTransferLock lock = mock(StateTransferLock.class);
      CompletableFuture<Void> topologyFuture = new CompletableFuture<>();
      when(handler.getStateTransferLock()).thenReturn(lock);
      when(lock.topologyFuture(anyInt())).thenReturn(topologyFuture);
      when(handler.isStopped()).thenReturn(false);

      DefaultTopologyRunnable runnable = new DefaultTopologyRunnable(handler, mock(CacheRpcCommand.class), Reply.NO_OP,
            TopologyMode.WAIT_TOPOLOGY, 5, true);
      CompletionStage<CacheNotFoundResponse> stage = runnable.beforeInvoke();
      assertThat(stage).isNotDone();

      // StateTransferLockImpl completes the waiting stages normally when the cache stops
      when(handler.isStopped()).thenReturn(true);
      topologyFuture.complete(null);

      assertThat(stage).isCompletedWithValue(CacheNotFoundResponse.INSTANCE);
   }
}
