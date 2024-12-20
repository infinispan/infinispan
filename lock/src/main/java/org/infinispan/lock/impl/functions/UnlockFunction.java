package org.infinispan.lock.impl.functions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.logging.Log;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Function that allows to unlock the lock, if it's not already released.
 * <p>
 * <p>
 * <ul>
 *    <li>If the requestor is not the owner, the lock won't be released. </li>
 *    <li>If the requestId is null, this value does not affect the unlock </li>
 *    <li>If the requestId is not null, the lock will be released only if the requestId and the owner match</li>
 *    <li>If lock is already released, nothing happens</li>
 * </ul>
 * <p>
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FUNCTION_UNLOCK)
public class UnlockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(UnlockFunction.class, Log.class);

   private final String requestId;
   private final Set<Object> requesters;

   public UnlockFunction(Object requestor) {
      this.requestId = null;
      this.requesters = Collections.singleton(requestor);
   }

   public UnlockFunction(String requestId, Set<Object> requesters) {
      this.requestId = requestId;
      this.requesters = requesters;
   }

   @ProtoFactory
   static UnlockFunction protoFactory(String requestId, Set<WrappedMessage> requesters) {
      return new UnlockFunction(
            requestId,
            requesters == null ? null : requesters.stream().map(WrappedMessage::getValue).collect(Collectors.toSet())
      );
   }

   @ProtoField(1)
   String getRequestId() {
      return requestId;
   }

   @ProtoField(value = 2, collectionImplementation = HashSet.class)
   Set<WrappedMessage> getRequesters() {
      return requesters == null ? null : requesters.stream().map(WrappedMessage::new).collect(Collectors.toSet());
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      if (log.isTraceEnabled()) {
         log.tracef("Lock[%s] unlock request by reqId [%s] requestors %s", entryView.key().getName(), requestId, requesters);
      }

      ClusteredLockValue lockValue = entryView.find().orElseThrow(() -> log.lockDeleted());

      // If the lock is already released return true
      if (lockValue.getState() == ClusteredLockState.RELEASED) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock[%s] Already free. State[RELEASED], reqId [%s], owner [%s]", entryView.key().getName(), lockValue.getRequestId(), lockValue.getOwner());
         }
         return Boolean.TRUE;
      }

      boolean requestIdMatches = requestId == null || (lockValue.getRequestId() != null && lockValue.getRequestId().equals(requestId));
      boolean ownerMatches = lockValue.getOwner() != null && requesters.contains(lockValue.getOwner());

      // If the requestId and the owner match, unlock and return true
      if (requestIdMatches && ownerMatches) {
         if (log.isTraceEnabled()) {
            log.tracef("Lock[%s] Unlocked by reqId [%s] requestors %s", entryView.key().getName(), requestId, requesters);
         }

         entryView.set(ClusteredLockValue.INITIAL_STATE);
         return Boolean.TRUE;
      }

      // Trace and return false if unlock is not possible
      if (log.isTraceEnabled()) {
         log.tracef("Lock[%s] Unlock not possible by reqId [%s] requestors %s. Current State[ACQUIRED], reqId [%s], owner [%s]",
               entryView.key().getName(),
               requestId,
               requesters,
               lockValue.getRequestId(),
               lockValue.getOwner());
      }

      return Boolean.FALSE;
   }
}
