package org.infinispan.lock.impl.functions;

import java.util.function.Function;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.logging.Log;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * IsLocked function that allows to know if a lock is already acquired. It returns {@link Boolean#TRUE} when the lock is
 * acquired and {@link Boolean#FALSE} when it is not.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FUNCTION_IS_LOCKED)
public class IsLocked implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(IsLocked.class, Log.class);

   private final Object requestor;

   public IsLocked() {
      requestor = null;
   }

   public IsLocked(Object requestor) {
      this.requestor = requestor;
   }

   @ProtoFactory
   static IsLocked protoFactory(WrappedMessage wrappedRequestor) {
      return new IsLocked(WrappedMessages.unwrap(wrappedRequestor));
   }

   @ProtoField(1)
   WrappedMessage getWrappedRequestor() {
      return WrappedMessages.orElseNull(requestor);
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      ClusteredLockValue lock = entryView.find().orElseThrow(() -> log.lockDeleted());
      Boolean result = Boolean.FALSE;
      if (lock.getState() == ClusteredLockState.ACQUIRED &&
            (requestor == null || (lock.getOwner() != null && lock.getOwner().equals(requestor)))) {
         result = Boolean.TRUE;
      }
      return result;
   }
}
