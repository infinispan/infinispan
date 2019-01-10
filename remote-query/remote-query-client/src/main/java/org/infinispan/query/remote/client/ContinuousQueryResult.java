package org.infinispan.query.remote.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult {

   public enum ResultType {
      JOINING,
      UPDATED,
      LEAVING;

      public static final class Marshaller implements EnumMarshaller<ResultType> {

         @Override
         public ResultType decode(int enumValue) {
            switch (enumValue) {
               case 0:
                  return ResultType.LEAVING;
               case 1:
                  return ResultType.JOINING;
               case 2:
                  return ResultType.UPDATED;
            }
            return null;
         }

         @Override
         public int encode(ResultType resultType) throws IllegalArgumentException {
            switch (resultType) {
               case LEAVING:
                  return 0;
               case JOINING:
                  return 1;
               case UPDATED:
                  return 2;
               default:
            }
            throw new IllegalArgumentException("Unexpected ResultType value : " + resultType);
         }

         @Override
         public Class<ResultType> getJavaClass() {
            return ResultType.class;
         }

         @Override
         public String getTypeName() {
            return "org.infinispan.query.remote.client.ContinuousQueryResult.ResultType";
         }
      }
   }

   private final ResultType resultType;

   private final byte[] key;

   private final byte[] value;

   private final Object[] projection;

   public ContinuousQueryResult(ResultType resultType, byte[] key, byte[] value, Object[] projection) {
      this.resultType = resultType;
      this.key = key;
      this.value = value;
      this.projection = projection;
   }

   public ResultType getResultType() {
      return resultType;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getValue() {
      return value;
   }

   public Object[] getProjection() {
      return projection;
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "resultType=" + resultType +
            ", key=" + Arrays.toString(key) +
            ", value=" + Arrays.toString(value) +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }

   public static final class Marshaller implements MessageMarshaller<ContinuousQueryResult> {

      @Override
      public ContinuousQueryResult readFrom(ProtoStreamReader reader) throws IOException {
         ResultType type = reader.readObject("resultType", ResultType.class);
         byte[] key = reader.readBytes("key");
         byte[] value = reader.readBytes("value");
         List<WrappedMessage> projection = reader.readCollection("projection", new ArrayList<>(), WrappedMessage.class);
         Object[] p = null;
         if (!projection.isEmpty()) {
            p = new Object[projection.size()];
            int j = 0;
            for (WrappedMessage m : projection) {
               p[j++] = m.getValue();
            }
         }
         return new ContinuousQueryResult(type, key, value, p);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, ContinuousQueryResult continuousQueryResult) throws IOException {
         writer.writeObject("resultType", continuousQueryResult.resultType, ResultType.class);
         writer.writeBytes("key", continuousQueryResult.key);
         if (continuousQueryResult.projection == null) {
            // skip marshalling the instance if there is a projection
            writer.writeBytes("value", continuousQueryResult.value);
         } else {
            WrappedMessage[] p = new WrappedMessage[continuousQueryResult.projection.length];
            for (int i = 0; i < p.length; i++) {
               p[i] = new WrappedMessage(continuousQueryResult.projection[i]);
            }
            writer.writeArray("projection", p, WrappedMessage.class);
         }
      }

      @Override
      public Class<ContinuousQueryResult> getJavaClass() {
         return ContinuousQueryResult.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.ContinuousQueryResult";
      }
   }
}
