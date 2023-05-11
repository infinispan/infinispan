package org.infinispan.statetransfer;

import io.reactivex.rxjava3.core.Flowable;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.test.TestException;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "statetransfer.OutboundTransferTaskTest")
@CleanupAfterMethod
public class OutboundTransferTaskTest {


   public void shouldNotifyForAllSegments() throws InterruptedException {
      int numSegments = 30;
      IntSet segments = IntSets.from(IntStream.range(0, numSegments).iterator());

      RpcManager rpcManager = mock(RpcManager.class);
      CommandsFactory commandsFactory = mock(CommandsFactory.class);

      OutboundTransferTask task = new OutboundTransferTask(
            LocalModeAddress.INSTANCE,
            segments,
            numSegments,
            numSegments,
            1,
            chunks -> {},
            rpcManager,
            commandsFactory,
            10_000,
            "mock-cache",
            true
      );

      ArgumentCaptor<Collection<StateChunk>> cmdCaptor = ArgumentCaptor.forClass(Collection.class);
      when(commandsFactory.buildStateResponseCommand(anyInt(), cmdCaptor.capture(), anyBoolean()))
            .thenReturn(mock(StateResponseCommand.class));
      when(rpcManager.invokeCommand(any(Address.class), any(), any(), any()))
            .thenAnswer(i -> CompletableFutures.completedNull());

      List<SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>>> entries = new ArrayList<>();
      for (int i = 0; i < numSegments; i++) {
         ImmortalCacheEntry entry = new ImmortalCacheEntry("key", "value");
         entries.add(Notifications.value(entry, i));
         entries.add(Notifications.segmentComplete(i));
      }

      final CountDownLatch latch = new CountDownLatch(1);
      task.execute(Flowable.fromIterable(entries))
            .whenComplete((v, t) -> {
               if (t != null) {
                  throw new TestException(t);
               }
               latch.countDown();
            });

      if (!latch.await(15, TimeUnit.SECONDS)) {
         throw new TestException("Did not receive all segment notifications");
      }

      // We have 30 segments, the flowable contains 1 notification for data and another for segment complete.
      // Since the chunk size is 30, we will issue 2 requests containing 15 chunks each.
      IntSet transferred = IntSets.mutableEmptySet(numSegments);
      assertEquals(cmdCaptor.getAllValues().size(), 2);
      for (Collection<StateChunk> chunks : cmdCaptor.getAllValues()) {
         assertEquals(chunks.size(), 15);
         transferred.addAll(chunks.stream().map(StateChunk::getSegmentId).collect(Collectors.toList()));
      }

      assertEquals(transferred, segments);
   }
}
