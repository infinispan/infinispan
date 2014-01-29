package org.infinispan.interceptors.distribution;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test to ensure proper behavior of {@link L1WriteSynchronizer}
 *
 * @author wburns
 * @since 6.0
 */
@Test(groups = "unit", testName = "interceptors.distribution.L1WriteSynchronizerTest")
public class L1WriteSynchronizerTest extends AbstractInfinispanTest {
   private L1WriteSynchronizer sync;
   private DataContainer dc;
   private StateTransferLock stl;
   private ClusteringDependentLogic cdl;
   private final long l1Timeout = 1000;


   @BeforeMethod
   public void beforeMethod() {
      dc = mock(DataContainer.class);
      stl = mock(StateTransferLock.class);
      cdl = mock(ClusteringDependentLogic.class);

      sync = new L1WriteSynchronizer(dc, l1Timeout, stl, cdl);
   }

   @Test
   public void testNullICEProvided() throws ExecutionException, InterruptedException {
      sync.runL1UpdateIfPossible(null);

      assertNull(sync.get());
   }

   @Test
   public void testNullICEProvidedWait() throws ExecutionException, InterruptedException, TimeoutException {
      sync.runL1UpdateIfPossible(null);

      assertNull(sync.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testGetReturnValueWait() throws InterruptedException, ExecutionException, TimeoutException {
      Object value = new Object();
      InternalCacheEntry ice = new ImmortalCacheEntry(value, value);
      sync.runL1UpdateIfPossible(ice);

      assertEquals(value, sync.get());
   }

   @Test
   public void testGetReturnValueTimeWait() throws InterruptedException, ExecutionException, TimeoutException {
      Object value = new Object();
      InternalCacheEntry ice = new ImmortalCacheEntry(value, value);
      sync.runL1UpdateIfPossible(ice);

      assertEquals(value, sync.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testExceptionWait() throws InterruptedException {
      Throwable t = mock(Throwable.class);
      sync.retrievalEncounteredException(t);

      try {
         sync.get();
      } catch (ExecutionException e) {
         assertEquals(t, e.getCause());
      }
   }

   @Test
   public void testExceptionTimeWait() throws InterruptedException, TimeoutException {
      Throwable t = mock(Throwable.class);
      sync.retrievalEncounteredException(t);

      try {
         sync.get(1, TimeUnit.SECONDS);
         fail("Should have thrown an execution exception");
      } catch (ExecutionException e) {
         assertEquals(t, e.getCause());
      }
   }

   @Test
   public void testSpawnedThreadBlockingValue() throws InterruptedException, ExecutionException, TimeoutException {
      Object value = new Object();
      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get();
         }
      });

      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      InternalCacheEntry ice = new ImmortalCacheEntry(value, value);
      sync.runL1UpdateIfPossible(ice);

      assertEquals(value, future.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testSpawnedThreadBlockingValueTimeWait() throws InterruptedException, ExecutionException, TimeoutException {
      Object value = new Object();
      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get(5, TimeUnit.SECONDS);
         }
      });

      // This should not return since we haven't signaled the sync yet
      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      InternalCacheEntry ice = new ImmortalCacheEntry(value, value);
      sync.runL1UpdateIfPossible(ice);

      assertEquals(value, future.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testSpawnedThreadBlockingNullValue() throws InterruptedException, ExecutionException, TimeoutException {
      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get();
         }
      });

      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      sync.runL1UpdateIfPossible(null);

      assertNull(future.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testSpawnedThreadBlockingNullValueTimeWait() throws InterruptedException, ExecutionException, TimeoutException {
      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get(5, TimeUnit.SECONDS);
         }
      });

      // This should not return since we haven't signaled the sync yet
      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      sync.runL1UpdateIfPossible(null);

      assertNull(future.get(1, TimeUnit.SECONDS));
   }

   @Test
   public void testSpawnedThreadBlockingException() throws InterruptedException, ExecutionException, TimeoutException {
      Throwable t = mock(Throwable.class);

      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get();
         }
      });

      // This should not return since we haven't signaled the sync yet
      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      sync.retrievalEncounteredException(t);

      try {
         sync.get(1, TimeUnit.SECONDS);
         fail("Should have thrown an execution exception");
      } catch (ExecutionException e) {
         assertEquals(t, e.getCause());
      }
   }

   @Test
   public void testSpawnedThreadBlockingExceptionTimeWait() throws InterruptedException, ExecutionException, TimeoutException {
      Throwable t = mock(Throwable.class);

      Future future = fork(new Callable<Object>() {

         @Override
         public Object call() throws Exception {
            return sync.get(5, TimeUnit.SECONDS);
         }
      });

      try {
         future.get(50, TimeUnit.MILLISECONDS);
         fail("Should have thrown a timeout exception");
      } catch (TimeoutException e) {
         // This should time out exception
      }

      sync.retrievalEncounteredException(t);

      try {
         sync.get(1, TimeUnit.SECONDS);
         fail("Should have thrown an execution exception");
      } catch (ExecutionException e) {
         assertEquals(t, e.getCause());
      }
   }

   @Test
   public void testWriteCancelled() {
      assertTrue(sync.trySkipL1Update());

      Object keyValue = new Object();
      InternalCacheEntry ice = new ImmortalCacheEntry(keyValue, keyValue);
      sync.runL1UpdateIfPossible(ice);

      // The dc shouldn't have been updated
      verify(dc, never()).put(any(), any(), any(Metadata.class));
   }

   @Test
   public void testWriteCannotCancel() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      final CyclicBarrier barrier = new CyclicBarrier(2);

      // We use the topology lock as a sync point to know when the write is being attempted - note this is after
      // the synchronizer has been marked as a write occurring
      doAnswer(new Answer<Void>() {

         @Override
         public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
            barrier.await();
            return null;
         }
      }).when(stl).acquireSharedTopologyLock();

      Future<Void> future = fork(new Runnable() {
         @Override
         public void run() {
            Object keyValue = new Object();
            InternalCacheEntry ice = new ImmortalCacheEntry(keyValue, keyValue);
            sync.runL1UpdateIfPossible(ice);
         }
      }, null);

      // wait for the thread to try updating
      barrier.await(1, TimeUnit.SECONDS);

      assertFalse(sync.trySkipL1Update());

      future.get(1, TimeUnit.SECONDS);

      // The dc should have been updated
      verify(dc).put(any(), any(), any(Metadata.class));
   }

   @Test
   public void testDCUpdatedHigherICELifespan() {
      verifyDCUpdate(Long.MAX_VALUE, false);
   }


   @Test
   public void testDCUpdatedLowerICELifespan() {
      verifyDCUpdate(50, true);
   }

   private void verifyDCUpdate(long iceLifespan, boolean shouldBeIceLifespan) {
      Object value = new Object();
      Object key = new Object();

      InternalCacheEntry ice = when(mock(InternalCacheEntry.class, RETURNS_DEEP_STUBS).getValue()).thenReturn(value).getMock();
      when(ice.getKey()).thenReturn(key);
      when(ice.getLifespan()).thenReturn(iceLifespan);

      sync.runL1UpdateIfPossible(ice);

      verify(dc).put(eq(key), eq(value), any(Metadata.class));

      Metadata.Builder verifier = verify(ice.getMetadata().builder());
      verifier.lifespan(shouldBeIceLifespan ? iceLifespan : l1Timeout);
      verifier.maxIdle(-1);
   }
}
