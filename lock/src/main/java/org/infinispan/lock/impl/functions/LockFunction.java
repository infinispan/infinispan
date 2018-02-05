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
import org.infinispan.lock.impl.log.Log;

/**
 * Lock function that allows to acquire the lock by a requestor, if such action is possible. It returns {@link
 * Boolean#TRUE} when the lock is acquired and {@link Boolean#FALSE} when it is not.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class LockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(LockFunction.class, Log.class);

   public static final AdvancedExternalizer<LockFunction> EXTERNALIZER = new Externalizer();
   private final String requestId;
   private final Object requestor;

   public LockFunction(String requestId, Object requestor) {
      this.requestId = requestId;
      this.requestor = requestor;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      ClusteredLockValue lock = entryView.find().orElseThrow(() -> log.lockDeleted());
      log.tracef("lock request by reqId %s requestor %s", requestId, requestor);
      if (lock.getState() == ClusteredLockState.RELEASED) {
         entryView.set(new ClusteredLockValue(requestId, requestor, ClusteredLockState.ACQUIRED));
         log.tracef("lock acquired by %s %s", requestId, requestor);
         return Boolean.TRUE;
      } else if (lock.getState() == ClusteredLockState.ACQUIRED && lock.getRequestId().equals(requestId) && lock.getOwner().equals(requestor)) {
         log.tracef("lock already acquired by %s %s", requestId, requestor);
         return Boolean.TRUE;
      }
      log.tracef("lock not available, owned by %s %s", lock.getRequestId(), lock.getOwner());
      return Boolean.FALSE;
   }

   private static class Externalizer implements AdvancedExternalizer<LockFunction> {

      @Override
      public Set<Class<? extends LockFunction>> getTypeClasses() {
         return Collections.singleton(LockFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.LOCK_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, LockFunction object) throws IOException {
         MarshallUtil.marshallString(object.requestId, output);
         output.writeObject(object.requestor);
      }

      @Override
      public LockFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new LockFunction(MarshallUtil.unmarshallString(input), input.readObject());
      }
   }
}
