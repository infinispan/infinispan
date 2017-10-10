package org.infinispan.lock.impl.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;

/**
 * Lock object inside the cache. Holds the lock owner, the lock request id and the status of the lock.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ClusteredLockValue {

   public static final ClusteredLockValue INITIAL_STATE = new ClusteredLockValue();
   public static final AdvancedExternalizer<ClusteredLockValue> EXTERNALIZER = new Externalizer();
   private final String requestId;
   private final Object owner;
   private final ClusteredLockState state;

   public ClusteredLockValue(String requestId, Object owner, ClusteredLockState state) {
      this.requestId = requestId;
      this.owner = owner;
      this.state = state;
   }

   private ClusteredLockValue() {
      this.requestId = null;
      this.owner = null;
      this.state = ClusteredLockState.RELEASED;
   }

   public ClusteredLockState getState() {
      return state;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      ClusteredLockValue that = (ClusteredLockValue) o;
      return Objects.equals(requestId, that.requestId) && Objects.equals(owner, that.owner) && Objects.equals(state, that.state);
   }

   @Override
   public int hashCode() {
      return Objects.hash(requestId, owner, state);
   }

   @Override
   public String toString() {
      return "ClusteredLockValue{" +
            " requestId=" + requestId +
            " owner=" + owner +
            " state=" + state +
            '}';
   }

   public String getRequestId() {
      return requestId;
   }

   public Object getOwner() {
      return owner;
   }

   private static class Externalizer implements AdvancedExternalizer<ClusteredLockValue> {

      @Override
      public Set<Class<? extends ClusteredLockValue>> getTypeClasses() {
         return Collections.singleton(ClusteredLockValue.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_LOCK_VALUE;
      }

      @Override
      public void writeObject(ObjectOutput output, ClusteredLockValue object) throws IOException {
         MarshallUtil.marshallString(object.requestId, output);
         output.writeObject(object.owner);
         MarshallUtil.marshallEnum(object.state, output);
      }

      @Override
      public ClusteredLockValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String requestId = MarshallUtil.unmarshallString(input);
         Object owner = input.readObject();
         ClusteredLockState state = MarshallUtil.unmarshallEnum(input, ClusteredLockState::valueOf);
         return new ClusteredLockValue(requestId, owner, state);
      }
   }
}
