package org.infinispan.query.remote.client;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult {

   public final boolean joining;

   public final byte[] key;

   public final byte[] value;

   public ContinuousQueryResult(boolean joining, byte[] key, byte[] value) {
      this.joining = joining;
      this.key = key;
      this.value = value;
   }

   public boolean isJoining() {
      return joining;
   }

   public byte[] getKey() {
      return key;
   }

   public byte[] getValue() {
      return value;
   }

   @Override
   public String toString() {
      return "ContinuousQueryResult{" +
            "joining=" + joining +
            ", key=" + Arrays.toString(key) +
            ", value=" + Arrays.toString(value) +
            '}';
   }

   public static final class Marshaller implements MessageMarshaller<ContinuousQueryResult> {

      @Override
      public ContinuousQueryResult readFrom(ProtoStreamReader reader) throws IOException {
         boolean joining = reader.readBoolean("joining");
         byte[] key = reader.readBytes("key");
         byte[] value = reader.readBytes("value");
         return new ContinuousQueryResult(joining, key, value);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, ContinuousQueryResult continuousQueryResult) throws IOException {
         writer.writeBoolean("joining", continuousQueryResult.isJoining());
         writer.writeBytes("key", continuousQueryResult.getKey());
         writer.writeBytes("value", continuousQueryResult.getValue());
      }

      @Override
      public Class<? extends ContinuousQueryResult> getJavaClass() {
         return ContinuousQueryResult.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.ContinuousQueryResult";
      }
   }
}
