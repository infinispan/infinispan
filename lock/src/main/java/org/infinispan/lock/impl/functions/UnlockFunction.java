package org.infinispan.lock.impl.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;
import org.infinispan.lock.logging.Log;

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
public class UnlockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(UnlockFunction.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   public static final AdvancedExternalizer<UnlockFunction> EXTERNALIZER = new Externalizer();

   private final String requestId;
   private final Set<Object> requestors;

   public UnlockFunction(Object requestor) {
      this.requestId = null;
      this.requestors = Collections.singleton(requestor);
   }

   public UnlockFunction(String requestId, Set<Object> requestors) {
      this.requestId = requestId;
      this.requestors = requestors;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      if (trace) {
         log.tracef("Lock[%s] unlock request by reqId [%s] requestors %s", entryView.key().getName(), requestId, requestors);
      }

      ClusteredLockValue lockValue = entryView.find().orElseThrow(() -> log.lockDeleted());

      // If the lock is already released return true
      if (lockValue.getState() == ClusteredLockState.RELEASED) {
         if (trace) {
            log.tracef("Lock[%s] Already free. State[RELEASED], reqId [%s], owner [%s]", entryView.key().getName(), lockValue.getRequestId(), lockValue.getOwner());
         }
         return Boolean.TRUE;
      }

      boolean requestIdMatches = requestId == null || (lockValue.getRequestId() != null && lockValue.getRequestId().equals(requestId));
      boolean ownerMatches = lockValue.getOwner() != null && requestors.contains(lockValue.getOwner());

      // If the requestId and the owner match, unlock and return true
      if (requestIdMatches && ownerMatches) {
         if (trace) {
            log.tracef("Lock[%s] Unlocked by reqId [%s] requestors %s", entryView.key().getName(), requestId, requestors);
         }

         entryView.set(ClusteredLockValue.INITIAL_STATE);
         return Boolean.TRUE;
      }

      // Trace and return false if unlock is not possible
      if (trace) {
         log.tracef("Lock[%s] Unlock not possible by reqId [%s] requestors %s. Current State[ACQUIRED], reqId [%s], owner [%s]",
               entryView.key().getName(),
               requestId,
               requestors,
               lockValue.getRequestId(),
               lockValue.getOwner());
      }

      return Boolean.FALSE;
   }

   private static class Externalizer implements AdvancedExternalizer<UnlockFunction> {

      @Override
      public Set<Class<? extends UnlockFunction>> getTypeClasses() {
         return Collections.singleton(UnlockFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.UNLOCK_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, UnlockFunction object) throws IOException {
         MarshallUtil.marshallString(object.requestId, output);
         output.writeObject(object.requestors);
      }

      @Override
      public UnlockFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnlockFunction(MarshallUtil.unmarshallString(input), (Set<Object>) input.readObject());
      }
   }
}
