package org.infinispan.lock.impl.functions;

import java.util.function.Function;

import org.infinispan.commons.logging.LogFactory;
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
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

/**
 * Lock function that allows to acquire the lock by a requestor, if such action is possible. It returns {@link
 * Boolean#TRUE} when the lock is acquired and {@link Boolean#FALSE} when it is not.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTERED_LOCK_FUNCTION_LOCK)
public class LockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(LockFunction.class, Log.class);

   private final String requestId;
   private final Address requestor;

   public LockFunction(String requestId, Address requestor) {
      this.requestId = requestId;
      this.requestor = requestor;
   }

   @ProtoFactory
   LockFunction(String requestId, JGroupsAddress requestor) {
      this(requestId, (Address) requestor);
   }

   @ProtoField(1)
   String getRequestId() {
      return requestId;
   }

   @ProtoField(value = 2, javaType = JGroupsAddress.class)
   Address getRequestor() {
      return requestor;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      ClusteredLockValue lock = entryView.find().orElseThrow(() -> log.lockDeleted());
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] lock request by reqId %s requestor %s", entryView.key().getName(), requestId, requestor);
      }
      if (lock.getState() == ClusteredLockState.RELEASED) {
         entryView.set(new ClusteredLockValue(requestId, requestor, ClusteredLockState.ACQUIRED));
         if (log.isTraceEnabled()) {
            log.tracef("LOCK[%s] lock acquired by %s %s", entryView.key().getName(), requestId, requestor);
         }
         return Boolean.TRUE;
      } else if (lock.getState() == ClusteredLockState.ACQUIRED && lock.getRequestId().equals(requestId) && lock.getOwner().equals(requestor)) {
         log.tracef("LOCK[%s] lock already acquired by %s %s", entryView.key().getName(), requestId, requestor);
         return Boolean.TRUE;
      }
      if (log.isTraceEnabled()) {
         log.tracef("LOCK[%s] lock not available, owned by %s %s", entryView.key().getName(), lock.getRequestId(), lock.getOwner());
      }
      return Boolean.FALSE;
   }
}
