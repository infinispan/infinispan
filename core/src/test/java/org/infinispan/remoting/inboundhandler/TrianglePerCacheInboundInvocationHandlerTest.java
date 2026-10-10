package org.infinispan.remoting.inboundhandler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "remoting.inboundhandler.TrianglePerCacheInboundInvocationHandlerTest")
public class TrianglePerCacheInboundInvocationHandlerTest extends AbstractInfinispanTest {

   public void testSkippedBackupWriteIsRetried() throws Throwable {
      CommandAckCollector collector = mock(CommandAckCollector.class);
      BackupWriteCommand command = mock(BackupWriteCommand.class);
      DefaultTopologyRunnable runnable = createRunnable(collector, command);

      // The superclass can reject the write when the cache stops before invocation.
      doReturn(CompletableFuture.completedFuture(CacheNotFoundResponse.INSTANCE)).when(runnable).beforeInvoke();
      runnable.run();

      verify(collector).completeExceptionally(1, OutdatedTopologyException.RETRY_NEXT_TOPOLOGY, 5);
      verifyNoMoreInteractions(collector);
      verify(command, never()).invokeAsync(any());
   }

   public void testAppliedBackupWriteIsAcknowledged() throws Throwable {
      CommandAckCollector collector = mock(CommandAckCollector.class);
      BackupWriteCommand command = mock(BackupWriteCommand.class);
      DefaultTopologyRunnable runnable = createRunnable(collector, command);

      doReturn(null).when(runnable).beforeInvoke();
      runnable.run();

      verify(collector).backupAck(1, command.getCommandInvocationId().getAddress(), 0, 5);
      verifyNoMoreInteractions(collector);
      verify(command).invokeAsync(any());
   }

   private DefaultTopologyRunnable createRunnable(CommandAckCollector collector, BackupWriteCommand command) throws Throwable {
      Address address = mock(Address.class);
      CommandInvocationId id = mock(CommandInvocationId.class);
      when(id.getAddress()).thenReturn(address);
      when(id.getId()).thenReturn(1L);
      when(command.getCommandInvocationId()).thenReturn(id);
      when(command.getTopologyId()).thenReturn(5);
      when(command.invokeAsync(any())).thenReturn(CompletableFuture.completedFuture(null));

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      TrianglePerCacheInboundInvocationHandler handler = new TrianglePerCacheInboundInvocationHandler();
      handler.configuration = config.build();
      handler.rpcManager = mock(RpcManager.class);
      when(handler.rpcManager.getAddress()).thenReturn(address);
      handler.blockingExecutor = mock(BlockingTaskAwareExecutorService.class);
      handler.nonBlockingExecutor = mock(BlockingTaskAwareExecutorService.class);
      handler.triangleOrderManager = mock(TriangleOrderManager.class);
      handler.commandAckCollector = collector;
      handler.responseGenerator = mock(ResponseGenerator.class);
      handler.start();
      handler.handle(command, Reply.NO_OP, DeliverOrder.NONE);

      ArgumentCaptor<BlockingRunnable> captor = ArgumentCaptor.forClass(BlockingRunnable.class);
      verify(handler.nonBlockingExecutor).execute(captor.capture());
      return spy((DefaultTopologyRunnable) captor.getValue());
   }
}
