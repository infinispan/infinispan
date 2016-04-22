package org.infinispan.query.remote.client;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.WrappedMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQueryResult {

   private final boolean isJoining;

   private final byte[] key;

   private final byte[] value;

   private final Object[] projection;

   public ContinuousQueryResult(boolean isJoining, byte[] key, byte[] value, Object[] projection) {
      this.isJoining = isJoining;
      this.key = key;
      this.value = value;
      this.projection = projection;
   }

   public boolean isJoining() {
      return isJoining;
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
            "isJoining=" + isJoining +
            ", key=" + Arrays.toString(key) +
            ", value=" + Arrays.toString(value) +
            ", projection=" + Arrays.toString(projection) +
            '}';
   }

   public static final class Marshaller implements MessageMarshaller<ContinuousQueryResult> {

      @Override
      public ContinuousQueryResult readFrom(ProtoStreamReader reader) throws IOException {
         boolean isJoining = reader.readBoolean("isJoining");
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
         return new ContinuousQueryResult(isJoining, key, value, p);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, ContinuousQueryResult continuousQueryResult) throws IOException {
         writer.writeBoolean("isJoining", continuousQueryResult.isJoining);
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
      public Class<? extends ContinuousQueryResult> getJavaClass() {
         return ContinuousQueryResult.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.ContinuousQueryResult";
      }
   }
}
