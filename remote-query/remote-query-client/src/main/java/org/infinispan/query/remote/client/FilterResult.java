package org.infinispan.query.remote.client;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.WrappedMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class FilterResult {

   private final Object instance;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
      if (instance == null && projection == null) {
         throw new IllegalArgumentException("instance and projection cannot be both null");
      }
      this.instance = instance;
      this.projection = projection;
      this.sortProjection = sortProjection;
   }

   public Object getInstance() {
      return instance;
   }

   public Object[] getProjection() {
      return projection;
   }

   public Comparable[] getSortProjection() {
      return sortProjection;
   }

   @Override
   public String toString() {
      return "FilterResult{" +
            "instance=" + instance +
            ", projection=" + Arrays.toString(projection) +
            ", sortProjection=" + Arrays.toString(sortProjection) +
            '}';
   }

   public static final class Marshaller implements MessageMarshaller<FilterResult> {

      @Override
      public FilterResult readFrom(ProtoStreamReader reader) throws IOException {
         byte[] instance = reader.readBytes("instance");
         List<WrappedMessage> projection = reader.readCollection("projection", new ArrayList<WrappedMessage>(), WrappedMessage.class);
         List<WrappedMessage> sortProjection = reader.readCollection("sortProjection", new ArrayList<WrappedMessage>(), WrappedMessage.class);

         Object i = null;
         if (instance != null) {
            i = ProtobufUtil.fromWrappedByteArray(reader.getSerializationContext(), instance);
         }

         Object[] p = null;
         if (!projection.isEmpty()) {
            p = new Object[projection.size()];
            int j = 0;
            for (WrappedMessage m : projection) {
               p[j++] = m.getValue();
            }
         }

         Comparable[] sp = null;
         if (!sortProjection.isEmpty()) {
            sp = new Comparable[sortProjection.size()];
            int j = 0;
            for (WrappedMessage m : sortProjection) {
               sp[j++] = (Comparable) m.getValue();
            }
         }

         return new FilterResult(i, p, sp);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, FilterResult filterResult) throws IOException {
         if (filterResult.getProjection() == null) {
            // skip marshalling the instance if there is a projection
            writer.writeBytes("instance", (byte[]) filterResult.getInstance());
         } else {
            WrappedMessage[] p = new WrappedMessage[filterResult.getProjection().length];
            for (int i = 0; i < p.length; i++) {
               p[i] = new WrappedMessage(filterResult.getProjection()[i]);
            }
            writer.writeArray("projection", p, WrappedMessage.class);
         }

         if (filterResult.getSortProjection() != null) {
            WrappedMessage[] p = new WrappedMessage[filterResult.getSortProjection().length];
            for (int i = 0; i < p.length; i++) {
               p[i] = new WrappedMessage(filterResult.getSortProjection()[i]);
            }
            writer.writeArray("sortProjection", p, WrappedMessage.class);
         }
      }

      @Override
      public Class<? extends FilterResult> getJavaClass() {
         return FilterResult.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.query.remote.client.FilterResult";
      }
   }
}
