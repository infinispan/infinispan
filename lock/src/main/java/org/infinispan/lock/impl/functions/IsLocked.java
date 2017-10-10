package org.infinispan.lock.impl.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;
import org.infinispan.lock.impl.log.Log;

/**
 * IsLocked function that allows to know if a lock is already acquired. It returns {@link Boolean#TRUE} when the lock is
 * acquired and {@link Boolean#FALSE} when it is not.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class IsLocked implements Function<EntryView.ReadWriteEntryView<ClusteredLockKey, ClusteredLockValue>, Boolean> {

   private static final Log log = LogFactory.getLog(IsLocked.class, Log.class);

   public static final AdvancedExternalizer<IsLocked> EXTERNALIZER = new Externalizer();

   private final Object requestor;

   public IsLocked() {
      requestor = null;
   }

   public IsLocked(Object requestor) {
      this.requestor = requestor;
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

   private static class Externalizer implements AdvancedExternalizer<IsLocked> {

      @Override
      public Set<Class<? extends IsLocked>> getTypeClasses() {
         return Collections.singleton(IsLocked.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.IS_LOCKED_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, IsLocked object) throws IOException {
         output.writeObject(object.requestor);
      }

      @Override
      public IsLocked readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IsLocked(input.readObject());
      }
   }
}
