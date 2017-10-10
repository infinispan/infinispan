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
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;
import org.infinispan.lock.impl.log.Log;

/**
 * Function that allows to unlock the lock, if it's not already released.
 *
 * <p>
 * <ul>
 *    <li>If the requestor is not the owner, the lock won't be released. </li>
 *    <li>If the requestId is null, this value does not affect the unlock </li>
 *    <li>If the requestId is not null, the lock will be released only if the requestId and the owner match </li>
 * </ul>
 * <p>
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class UnlockFunction implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Void> {

   private static final Log log = LogFactory.getLog(UnlockFunction.class, Log.class);

   public static final AdvancedExternalizer<UnlockFunction> EXTERNALIZER = new Externalizer();

   private final String requestId;
   private final Object requestor;

   public UnlockFunction(Object requestor) {
      this.requestId = null;
      this.requestor = requestor;
   }

   public UnlockFunction(String requestId, Object requestor) {
      this.requestId = requestId;
      this.requestor = requestor;
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue> entryView) {
      ClusteredLockValue lockValue = entryView.find().orElseThrow(() -> log.lockDeleted());
      boolean requestIdMatches = requestId == null || (lockValue.getRequestId() != null && lockValue.getRequestId().equals(requestId));
      boolean ownerMatches = lockValue.getOwner() != null && lockValue.getOwner().equals(requestor);
      if (requestIdMatches && ownerMatches) {
         entryView.set(ClusteredLockValue.INITIAL_STATE);
      }
      return null;
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
         output.writeObject(object.requestor);
      }

      @Override
      public UnlockFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnlockFunction(MarshallUtil.unmarshallString(input), input.readObject());
      }
   }
}
