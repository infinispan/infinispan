package org.infinispan.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.lock.ClusteredLockFilter;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * The lock value, the lock function and the cluster listener filter travel between the members of the cluster, so
 * they have to survive a marshalling round trip with the global marshaller.
 */
@Test(groups = "functional", testName = "clusteredLock.ClusteredLockMarshallingTest")
public class ClusteredLockMarshallingTest extends BaseClusteredLockTest {

   @Override
   protected int clusterSize() {
      return 2;
   }

   public void testReleasedLockValue() throws Exception {
      assertEquals(ClusteredLockValue.INITIAL_STATE, roundTrip(ClusteredLockValue.INITIAL_STATE));
   }

   public void testAcquiredLockValueWithoutWaiters() throws Exception {
      ClusteredLockValue value = ClusteredLockValue.acquire("request-id", address(0), false);
      ClusteredLockValue copy = roundTrip(value);

      assertEquals(value, copy);
      assertEquals("request-id", copy.getRequestId());
      assertEquals(address(0), copy.getOwner());
      assertEquals(ClusteredLockState.ACQUIRED, copy.getState());
      assertEquals(Set.of(), copy.getWaiters());
   }

   public void testLockValueWithWaiters() throws Exception {
      ClusteredLockValue value = ClusteredLockValue.acquire("request-id", address(0), false)
            .addWaiter(ClusteredLockValue.waiterId(address(1)))
            .addWaiter(Integer.MIN_VALUE)
            .addWaiter(Integer.MAX_VALUE);
      ClusteredLockValue copy = roundTrip(value);

      assertEquals(value, copy);
      assertEquals(value.getWaiters(), copy.getWaiters());
      assertTrue(copy.hasWaiter(ClusteredLockValue.waiterId(address(1))));
   }

   public void testReleasedLockValueKeepsWaiters() throws Exception {
      ClusteredLockValue released = ClusteredLockValue.acquire("request-id", address(0), false)
            .addWaiter(ClusteredLockValue.waiterId(address(1)))
            .release();
      ClusteredLockValue copy = roundTrip(released);

      assertEquals(released, copy);
      assertEquals(ClusteredLockState.RELEASED, copy.getState());
      assertTrue(copy.hasWaiter(ClusteredLockValue.waiterId(address(1))));
   }

   public void testLockFilter() throws Exception {
      int waiterId = ClusteredLockValue.waiterId(address(1));
      ClusteredLockFilter copy = roundTrip(new ClusteredLockFilter(waiterId));

      ClusteredLockValue released = ClusteredLockValue.acquire("request-id", address(0), false)
            .addWaiter(waiterId)
            .release();
      EventType modified = new EventType(false, false, Event.Type.CACHE_ENTRY_MODIFIED);
      assertTrue(copy.accept(null, null, null, released, null, modified));
      assertFalse(new ClusteredLockFilter(waiterId + 1).accept(null, null, null, released, null, modified));
   }

   public void testLockFunction() throws Exception {
      LockFunction copy = roundTrip(new LockFunction("request-id", address(0), true, true));

      assertEquals("request-id", TestingUtil.extractField(copy, "requestId"));
      assertEquals(address(0), TestingUtil.extractField(copy, "requestor"));
      assertEquals(Boolean.TRUE, TestingUtil.extractField(copy, "waiter"));
      assertEquals(Boolean.TRUE, TestingUtil.extractField(copy, "keepWaiter"));

      LockFunction withoutWaiter = roundTrip(new LockFunction("request-id", address(0)));
      assertEquals(Boolean.FALSE, TestingUtil.extractField(withoutWaiter, "waiter"));
      assertEquals(Boolean.FALSE, TestingUtil.extractField(withoutWaiter, "keepWaiter"));
   }

   @SuppressWarnings("unchecked")
   private <T> T roundTrip(T object) throws Exception {
      Marshaller marshaller = TestingUtil.extractGlobalMarshaller(manager(0));
      return (T) marshaller.objectFromByteBuffer(marshaller.objectToByteBuffer(object));
   }
}
