package org.infinispan.query.continuous.impl;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult<V> {

   private final boolean isJoining;

   private final V value;

   private final Object[] projection;

   ContinuousQueryResult(boolean isJoining, V value, Object[] projection) {
      this.isJoining = isJoining;
      this.value = value;
      this.projection = projection;
   }

   public boolean isJoining() {
      return isJoining;
   }

   public V getValue() {
      return value;
   }

   public Object[] getProjection() {
      return projection;
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "isJoining=" + isJoining +
            ", value=" + value +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }

   public static final class Externalizer extends AbstractExternalizer<ContinuousQueryResult> {

      @Override
      public void writeObject(ObjectOutput output, ContinuousQueryResult continuousQueryResult) throws IOException {
         output.writeBoolean(continuousQueryResult.isJoining);
         if (continuousQueryResult.isJoining) {
            if (continuousQueryResult.projection != null) {
               // skip serializing the instance if there is a projection
               output.writeObject(null);
               output.writeObject(continuousQueryResult.projection);
            } else {
               output.writeObject(continuousQueryResult.value);
            }
         }
      }

      @Override
      public ContinuousQueryResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean isJoining = input.readBoolean();
         Object value = null;
         Object[] projection = null;
         if (isJoining) {
            value = input.readObject();
            if (value == null) {
               projection = (Object[]) input.readObject();
            }
         }
         return new ContinuousQueryResult(isJoining, value, projection);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_RESULT;
      }

      @Override
      public Set<Class<? extends ContinuousQueryResult>> getTypeClasses() {
         return Collections.singleton(ContinuousQueryResult.class);
      }
   }
}
