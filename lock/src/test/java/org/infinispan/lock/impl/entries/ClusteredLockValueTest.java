package org.infinispan.lock.impl.entries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "clusteredLock.ClusteredLockValueTest")
public class ClusteredLockValueTest extends AbstractInfinispanTest {

   private static final Address NODE_A = Address.random("A");
   private static final Address NODE_B = Address.random("B");

   public void testInitialState() {
      assertNull(ClusteredLockValue.INITIAL_STATE.getOwner());
      assertNull(ClusteredLockValue.INITIAL_STATE.getRequestId());
      assertEquals(ClusteredLockState.RELEASED, ClusteredLockValue.INITIAL_STATE.getState());
      assertEquals(Set.of(), ClusteredLockValue.INITIAL_STATE.getWaiters());
   }

   public void testWaiterIdIsStableAndNodeSpecific() {
      assertEquals(ClusteredLockValue.waiterId(NODE_A), ClusteredLockValue.waiterId(NODE_A));
      // The id is derived from the node UUID so an equal address always yields the same id
      assertEquals(ClusteredLockValue.waiterId(NODE_A), ClusteredLockValue.waiterId(Address.fromNodeUUID(NODE_A.getNodeUUID())));
      assertFalse(ClusteredLockValue.waiterId(NODE_A) == ClusteredLockValue.waiterId(NODE_B));
   }

   public void testAcquireStartsANewEpoch() {
      ClusteredLockValue acquired = ClusteredLockValue.acquire("req", NODE_A, false)
            .addWaiter(ClusteredLockValue.waiterId(NODE_B));
      assertTrue(acquired.hasWaiter(ClusteredLockValue.waiterId(NODE_B)));

      // Acquiring again drops the waiters of the previous ownership epoch
      ClusteredLockValue reacquired = ClusteredLockValue.acquire("req2", NODE_B, false);
      assertEquals(ClusteredLockState.ACQUIRED, reacquired.getState());
      assertEquals(NODE_B, reacquired.getOwner());
      assertEquals(Set.of(), reacquired.getWaiters());
   }

   public void testAcquireKeepingTheOwnerAsWaiter() {
      ClusteredLockValue acquired = ClusteredLockValue.acquire("req", NODE_A, true);
      assertEquals(Set.of(ClusteredLockValue.waiterId(NODE_A)), acquired.getWaiters());
      assertFalse(acquired.hasWaiter(ClusteredLockValue.waiterId(NODE_B)));
   }

   public void testAddWaiterIsIdempotent() {
      ClusteredLockValue acquired = ClusteredLockValue.acquire("req", NODE_A, false);
      ClusteredLockValue withWaiter = acquired.addWaiter(ClusteredLockValue.waiterId(NODE_B));

      assertNotSame(acquired, withWaiter);
      assertFalse(acquired.hasWaiter(ClusteredLockValue.waiterId(NODE_B)));
      assertSame(withWaiter, withWaiter.addWaiter(ClusteredLockValue.waiterId(NODE_B)));
   }

   public void testReleaseKeepsTheWaiters() {
      ClusteredLockValue released = ClusteredLockValue.acquire("req", NODE_A, false)
            .addWaiter(ClusteredLockValue.waiterId(NODE_B))
            .release();

      assertEquals(ClusteredLockState.RELEASED, released.getState());
      assertNull(released.getOwner());
      assertNull(released.getRequestId());
      assertTrue(released.hasWaiter(ClusteredLockValue.waiterId(NODE_B)));
   }

   public void testReleaseWithoutWaiters() {
      assertSame(ClusteredLockValue.INITIAL_STATE, ClusteredLockValue.acquire("req", NODE_A, false).release());
      assertSame(ClusteredLockValue.INITIAL_STATE, ClusteredLockValue.INITIAL_STATE.release());
   }

   public void testEqualsIncludesWaiters() {
      ClusteredLockValue acquired = ClusteredLockValue.acquire("req", NODE_A, false);
      assertEquals(acquired, ClusteredLockValue.acquire("req", NODE_A, false));
      assertFalse(acquired.equals(acquired.addWaiter(ClusteredLockValue.waiterId(NODE_B))));
   }

   public void testNullWaitersAreNormalized() {
      ClusteredLockValue value = new ClusteredLockValue("req", NODE_A, ClusteredLockState.ACQUIRED, null);
      assertEquals(Set.of(), value.getWaiters());
      assertFalse(value.hasWaiter(ClusteredLockValue.waiterId(NODE_A)));
   }
}
