package org.infinispan.query.continuous.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult<V> {

   public enum ResultType {
      JOINING,
      UPDATED,
      LEAVING
   }

   private final ResultType resultType;

   private final V value;

   private final Object[] projection;

   ContinuousQueryResult(ResultType resultType, V value, Object[] projection) {
      this.resultType = resultType;
      this.value = value;
      this.projection = projection;
   }

   public ResultType getResultType() {
      return resultType;
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
            "resultType=" + resultType +
            ", value=" + value +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }

   public static final class Externalizer extends AbstractExternalizer<ContinuousQueryResult> {

      @Override
      public void writeObject(ObjectOutput output, ContinuousQueryResult continuousQueryResult) throws IOException {
         output.writeInt(continuousQueryResult.resultType.ordinal());
         if (continuousQueryResult.resultType != ResultType.LEAVING) {
            if (continuousQueryResult.projection != null) {
               // skip serializing the instance if there is a projection
               output.writeObject(null);
               int projLen = continuousQueryResult.projection.length;
               output.writeInt(projLen);
               for (int i = 0; i < projLen; i++) {
                  output.writeObject(continuousQueryResult.projection[i]);
               }
            } else {
               output.writeObject(continuousQueryResult.value);
            }
         }
      }

      @Override
      public ContinuousQueryResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ResultType type = ResultType.values()[input.readInt()];
         Object value = null;
         Object[] projection = null;
         if (type != ResultType.LEAVING) {
            value = input.readObject();
            if (value == null) {
               int projLen = input.readInt();
               projection = new Object[projLen];
               for (int i = 0; i < projLen; i++) {
                  projection[i] = input.readObject();
               }
            }
         }
         return new ContinuousQueryResult<>(type, value, projection);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_CONTINUOUS_QUERY_RESULT;
      }

      @Override
      public Set<Class<? extends ContinuousQueryResult>> getTypeClasses() {
         return Collections.singleton(ContinuousQueryResult.class);
      }
   }
}
