package org.infinispan.query.continuous.impl;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult<V> {

   private final boolean joining;

   private final V value;

   public ContinuousQueryResult(boolean joining, V value) {
      this.joining = joining;
      this.value = value;
   }

   public boolean isJoining() {
      return joining;
   }

   public V getValue() {
      return value;
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "joining=" + joining +
            ", value=" + value +
            '}';
   }

   public static final class Externalizer extends AbstractExternalizer<ContinuousQueryResult> {

      @Override
      public void writeObject(ObjectOutput output, ContinuousQueryResult continuousQueryResult) throws IOException {
         output.writeBoolean(continuousQueryResult.joining);
         if (continuousQueryResult.joining) {
            output.writeObject(continuousQueryResult.value);
         }
      }

      @Override
      public ContinuousQueryResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean joining = input.readBoolean();
         Object value = joining ? input.readObject() : null;
         return new ContinuousQueryResult(joining, value);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_RESULT;
      }

      @Override
      public Set<Class<? extends ContinuousQueryResult>> getTypeClasses() {
         return Collections.<Class<? extends ContinuousQueryResult>>singleton(ContinuousQueryResult.class);
      }
   }
}
