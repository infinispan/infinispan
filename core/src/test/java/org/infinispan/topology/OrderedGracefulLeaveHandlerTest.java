package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "topology.OrderedGracefulLeaveHandlerTest")
public class OrderedGracefulLeaveHandlerTest {

   private static final String CACHE_NAME = "test-cache";

   private OrderedGracefulLeaveHandler handler;
   private StateTransferTracker tracker;
   private AtomicInteger topologyId;
   private ManualExecutor executor;

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      handler = new OrderedGracefulLeaveHandler();
      tracker = new StateTransferTracker();
      topologyId = new AtomicInteger(1);
      executor = new ManualExecutor();
      ControlledTimeService timeService = new ControlledTimeService();

      BlockingManager bm = mock(BlockingManager.class);
      when(bm.asExecutor(any())).thenReturn(executor);
      TestingUtil.inject(handler, bm, tracker, timeService);
   }

   public void testSingleLeaveCompletesAfterRebalance() {
      ClusterCacheStatus status = mockStatus();

      CompletableFuture<Void> result = handler
            .enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, status)
            .toCompletableFuture();

      executor.runAll();
      assertThat(result).isNotDone();

      completeRebalance();

      assertThat(result).isCompleted();
   }

   public void testConcurrentLeavesAreSerialized() {
      AtomicInteger leaveOrder = new AtomicInteger(0);
      int[] observedOrder = new int[2];

      ClusterCacheStatus statusA = mockStatus(() -> observedOrder[0] = leaveOrder.incrementAndGet());
      ClusterCacheStatus statusB = mockStatus(() -> observedOrder[1] = leaveOrder.incrementAndGet());

      CompletableFuture<Void> resultA = handler
            .enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, statusA)
            .toCompletableFuture();

      CompletableFuture<Void> resultB = handler
            .enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, statusB)
            .toCompletableFuture();

      executor.runAll();

      assertThat(resultA).isNotDone();
      assertThat(resultB).isNotDone();
      assertThat(observedOrder[0]).isEqualTo(1);
      assertThat(observedOrder[1]).isEqualTo(0);

      completeRebalance();
      assertThat(resultA).isCompleted();

      // B's doLeave was queued by the chain advancement, drain it.
      executor.runAll();

      assertThat(observedOrder[1]).isEqualTo(2);
      assertThat(resultB).isNotDone();

      completeRebalance();
      assertThat(resultB).isCompleted();
   }

   public void testTimeoutAdvancesChain() throws Exception {
      ClusterCacheStatus statusA = mockStatus();
      ClusterCacheStatus statusB = mockStatus();

      CompletableFuture<Void> resultA = handler
            .enqueue(CACHE_NAME, mock(Address.class), 1, TimeUnit.NANOSECONDS, statusA)
            .toCompletableFuture();

      CompletableFuture<Void> resultB = handler
            .enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, statusB)
            .toCompletableFuture();

      executor.runAll();

      // Wait for the timeout to fire asynchronously.
      resultA.exceptionally(t -> null).toCompletableFuture().get(5, TimeUnit.SECONDS);

      assertThat(resultA).isCompletedExceptionally();
      assertThat(resultA).failsWithin(0, TimeUnit.SECONDS)
            .withThrowableThat().withCauseInstanceOf(TimeoutException.class);

      // B's doLeave was queued by the chain advancement after A timed out.
      executor.runAll();
      assertThat(resultB).isNotDone();

      completeRebalance();
      assertThat(resultB).isCompleted();
   }

   public void testRemoveCleansUpChain() {
      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());
      executor.runAll();
      handler.remove(CACHE_NAME);

      CompletableFuture<Void> result = handler
            .enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus())
            .toCompletableFuture();

      executor.runAll();
      assertThat(result).isNotDone();
   }

   public void testDrainAllCompletesWhenChainsFinish() throws Exception {
      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());
      executor.runAll();

      CompletableFuture<Boolean> drain = CompletableFuture.supplyAsync(() -> {
         try {
            return handler.drainAll(10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      });

      assertThat(drain).isNotDone();

      completeRebalance();

      assertThat(drain.get(10, TimeUnit.SECONDS)).isTrue();
   }

   public void testDrainAllReturnsFalseOnTimeout() throws InterruptedException {
      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());
      executor.runAll();

      assertThat(handler.drainAll(0, TimeUnit.SECONDS)).isFalse();
   }

   public void testDrainAllIncludesConcurrentLeaves() throws Exception {
      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());
      executor.runAll();

      CompletableFuture<Boolean> drain = CompletableFuture.supplyAsync(() -> {
         try {
            return handler.drainAll(10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      });

      assertThat(drain).isNotDone();

      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());
      completeRebalance();
      executor.runAll();

      assertThat(drain).isNotDone();

      completeRebalance();

      assertThat(drain.get(10, TimeUnit.SECONDS)).isTrue();
   }

   public void testDrainAllToleratesFailedChain() throws Exception {
      handler.enqueue(CACHE_NAME, mock(Address.class), 1, TimeUnit.NANOSECONDS, mockStatus());
      executor.runAll();

      handler.enqueue(CACHE_NAME, mock(Address.class), 10, TimeUnit.SECONDS, mockStatus());

      CompletableFuture<Boolean> drain = CompletableFuture.supplyAsync(() -> {
         try {
            return handler.drainAll(10, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      });

      assertThat(drain).isNotDone();

      executor.runAll();
      assertThat(drain).isNotDone();

      completeRebalance();

      assertThat(drain.get(10, TimeUnit.SECONDS)).isTrue();
   }

   private ClusterCacheStatus mockStatus() {
      return mockStatus(() -> {});
   }

   private ClusterCacheStatus mockStatus(Runnable onDoLeave) {
      ClusterCacheStatus status = mock(ClusterCacheStatus.class);
      CacheTopology ct = mock(CacheTopology.class);
      doAnswer(invocation -> {
         onDoLeave.run();
         int id = topologyId.incrementAndGet();
         when(ct.getTopologyId()).thenReturn(id);
         when(ct.getPendingCH()).thenReturn(mock(ConsistentHash.class));
         return null;
      }).when(status).doLeave(any());
      when(status.getCurrentTopology()).thenReturn(ct);
      return status;
   }

   private void completeRebalance() {
      int id = topologyId.incrementAndGet();
      CacheTopology stable = mock(CacheTopology.class);
      when(stable.getTopologyId()).thenReturn(id);
      when(stable.getPendingCH()).thenReturn(null);
      tracker.forCache(CACHE_NAME).cacheTopologyUpdated(stable);
   }

   /**
    * Executor that queues tasks for manual draining.
    *
    * <p>
    * Decouples chain advancement from tracker listener notification, avoiding reentrant modification of the tracker's listener list.
    * </p>
    */
   private static final class ManualExecutor implements Executor {
      private final Queue<Runnable> tasks = new ArrayDeque<>();

      @Override
      public void execute(Runnable command) {
         tasks.add(command);
      }

      public void runAll() {
         Runnable task;
         while ((task = tasks.poll()) != null) {
            task.run();
         }
      }
   }
}
