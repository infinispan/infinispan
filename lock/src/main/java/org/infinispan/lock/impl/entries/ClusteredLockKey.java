package org.infinispan.lock.impl.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;
import org.infinispan.util.ByteString;

/**
 * Used to retrieve and identify a lock in the cache
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ClusteredLockKey {
   public static final AdvancedExternalizer<ClusteredLockKey> EXTERNALIZER = new Externalizer();

   private final ByteString name;

   public ClusteredLockKey(ByteString name) {
      this.name = Objects.requireNonNull(name);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      ClusteredLockKey that = (ClusteredLockKey) o;

      return name.equals(that.name);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name);
   }

   @Override
   public String toString() {
      return "ClusteredLockKey{" +
            "name=" + name +
            '}';
   }

   public ByteString getName() {
      return name;
   }

   private static class Externalizer implements AdvancedExternalizer<ClusteredLockKey> {

      private Externalizer() {
      }

      @Override
      public Set<Class<? extends ClusteredLockKey>> getTypeClasses() {
         return Collections.singleton(ClusteredLockKey.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_LOCK_KEY;
      }

      @Override
      public void writeObject(ObjectOutput output, ClusteredLockKey object) throws IOException {
         ByteString.writeObject(output, object.name);
      }

      @Override
      public ClusteredLockKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClusteredLockKey(ByteString.readObject(input));
      }
   }
}
