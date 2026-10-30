package org.infinispan.lock.impl.functions;

import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.logging.Log;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Lock function that allows to acquire the lock by a requestor, if such action is possible. It returns {@link
 * Boolean#TRUE} when the lock is acquired and {@link Boolean#FALSE} when it is not.
 * <p>
 * The function also maintains the set of nodes waiting for the lock, which is what the single cluster listener of
 * each node uses to decide whether an event has to be delivered. A node is registered as a waiter when it cannot
 * acquire the lock and {@code waiter} is set, and it stays registered after a successful acquisition when
 * {@code keepWaiter} is set, which means that the node has more lock requests waiting to be served.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FUNCTION_LOCK)
public class LockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = Log.getLog(LockFunction.class);

   private final String requestId;
   private final Address requestor;
   private final boolean waiter;
   private final boolean keepWaiter;

   public LockFunction(String requestId, Address requestor) {
      this(requestId, requestor, false, false);
   }

   @ProtoFactory
   public LockFunction(String requestId, Address requestor, boolean waiter, boolean keepWaiter) {
      this.requestId = requestId;
      this.requestor = requestor;
      this.waiter = waiter || keepWaiter;
      this.keepWaiter = keepWaiter;
   }

   @ProtoField(1)
   String getRequestId() {
      return requestId;
   }

   @ProtoField(2)
   Address getRequestor() {
      return requestor;
   }

   @ProtoField(value = 3, defaultValue = "false")
   boolean isWaiter() {
      return waiter;
   }

   @ProtoField(value = 4, defaultValue = "false")
   boolean isKeepWaiter() {
      return keepWaiter;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      ClusteredLockValue lock = entryView.find().orElseThrow(log::lockDeleted);
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] lock request by reqId %s requestor %s", entryView.key().getName(), requestId, requestor);
      }
      if (lock.getState() == ClusteredLockState.RELEASED) {
         entryView.set(ClusteredLockValue.acquire(requestId, requestor, keepWaiter));
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] lock acquired by %s %s", entryView.key().getName(), requestId, requestor);
         }
         return Boolean.TRUE;
      } else if (lock.getState() == ClusteredLockState.ACQUIRED && requestId.equals(lock.getRequestId()) && requestor.equals(lock.getOwner())) {
         log.tracef("LOCK[%s] lock already acquired by %s %s", entryView.key().getName(), requestId, requestor);
         if (keepWaiter) {
            registerWaiter(entryView, lock);
         }
         return Boolean.TRUE;
      }
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] lock not available, owned by %s %s", entryView.key().getName(), lock.getRequestId(), lock.getOwner());
      }
      if (waiter) {
         registerWaiter(entryView, lock);
      }
      return Boolean.FALSE;
   }

   private void registerWaiter(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView, ClusteredLockValue lock) {
      ClusteredLockValue updated = lock.addWaiter(ClusteredLockValue.waiterId(requestor));
      if (updated != lock) {
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] %s registered as a waiter", entryView.key().getName(), requestor);
         }
         entryView.set(updated);
      }
   }
}
