package org.infinispan.lock.impl.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.ByteString;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "clusteredLock.ClusteredLockFilterTest")
public class ClusteredLockFilterTest extends AbstractInfinispanTest {

   private static final ClusteredLockKey KEY = new ClusteredLockKey(ByteString.fromString("ClusteredLockFilterTest"));
   private static final Address OWNER = Address.random("owner");
   private static final Address WAITER = Address.random("waiter");
   private static final Address STRANGER = Address.random("stranger");

   private static final EventType MODIFIED = new EventType(false, false, Event.Type.CACHE_ENTRY_MODIFIED);
   private static final EventType CREATED = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);
   private static final EventType REMOVED = new EventType(false, false, Event.Type.CACHE_ENTRY_REMOVED);

   private final ClusteredLockFilter filter = new ClusteredLockFilter(ClusteredLockValue.waiterId(WAITER));

   public void testReleasedLockIsNotifiedToTheWaitersOnly() {
      ClusteredLockValue acquired = acquiredWithWaiter();
      assertTrue(accept(acquired, acquired.release(), MODIFIED));

      ClusteredLockFilter strangerFilter = new ClusteredLockFilter(ClusteredLockValue.waiterId(STRANGER));
      assertFalse(strangerFilter.accept(KEY, acquired, null, acquired.release(), null, MODIFIED));
   }

   public void testWaiterRegistrationIsNotNotified() {
      // Registering a waiter modifies the entry but the lock is still acquired, nobody must be woken up
      ClusteredLockValue acquired = ClusteredLockValue.acquire("req", OWNER, false);
      assertFalse(accept(acquired, acquiredWithWaiter(), MODIFIED));
   }

   public void testAcquisitionIsNotNotified() {
      ClusteredLockValue released = acquiredWithWaiter().release();
      assertFalse(accept(released, ClusteredLockValue.acquire("req2", STRANGER, false), MODIFIED));
   }

   public void testLockCreationIsNotNotified() {
      assertFalse(accept(null, ClusteredLockValue.INITIAL_STATE, CREATED));
   }

   public void testRemovedLockIsNotifiedToTheWaitersOnly() {
      assertTrue(accept(acquiredWithWaiter(), null, REMOVED));
      assertFalse(accept(ClusteredLockValue.acquire("req", OWNER, false), null, REMOVED));
      assertFalse(accept(null, null, REMOVED));
   }

   private ClusteredLockValue acquiredWithWaiter() {
      return ClusteredLockValue.acquire("req", OWNER, false).addWaiter(ClusteredLockValue.waiterId(WAITER));
   }

   private boolean accept(ClusteredLockValue oldValue, ClusteredLockValue newValue, EventType eventType) {
      return filter.accept(KEY, oldValue, null, newValue, null, eventType);
   }
}
