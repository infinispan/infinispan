package org.infinispan.atomic;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.marshall.core.Ids;

/**
 * DeltaCompositeKey is the key guarding access to a specific entry in DeltaAware
 * @deprecated since 9.1
 */
@Deprecated
public final class DeltaCompositeKey {

   private final Object deltaAwareValueKey;
   private final Object entryKey;

   public DeltaCompositeKey(Object deltaAwareValueKey, Object entryKey) {
      if (deltaAwareValueKey == null || entryKey == null)
         throw new IllegalArgumentException("Keys cannot be null");

      this.deltaAwareValueKey = deltaAwareValueKey;
      this.entryKey = entryKey;
   }

   public final Object getDeltaAwareValueKey() {
      return deltaAwareValueKey;
   }

   @Override
   public int hashCode() {
      return 31 * deltaAwareValueKey.hashCode() + entryKey.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof DeltaCompositeKey)) {
         return false;
      }
      DeltaCompositeKey other = (DeltaCompositeKey) obj;
      return deltaAwareValueKey.equals(other.deltaAwareValueKey) && entryKey.equals(other.entryKey);
   }

   @Override
   public String toString() {
      return "DeltaCompositeKey[deltaAwareValueKey=" + deltaAwareValueKey + ", entryKey=" + entryKey + ']';
   }

   @Deprecated
   public static class DeltaCompositeKeyExternalizer extends AbstractExternalizer<DeltaCompositeKey> {

      @Override
      public void writeObject(UserObjectOutput output, DeltaCompositeKey dck) throws IOException {
         output.writeObject(dck.deltaAwareValueKey);
         output.writeObject(dck.entryKey);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DeltaCompositeKey readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Object deltaAwareValueKey = unmarshaller.readObject();
         Object entryKey = unmarshaller.readObject();
         return new DeltaCompositeKey(deltaAwareValueKey, entryKey);
      }

      @Override
      public Integer getId() {
         return Ids.DELTA_COMPOSITE_KEY;
      }

      @Override
      public Set<Class<? extends DeltaCompositeKey>> getTypeClasses() {
         return Collections.<Class<? extends DeltaCompositeKey>>singleton(DeltaCompositeKey.class);
      }
   }
}
