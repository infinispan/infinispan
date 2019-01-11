package org.infinispan.query.remote.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.remote.client.FilterResult;

/**
 * Protostream marshaller for {@link FilterResult}.
 *
 * @author anistor@redhat.com
 */
final class FilterResultMarshaller implements MessageMarshaller<FilterResult> {

   @Override
   public FilterResult readFrom(ProtoStreamReader reader) throws IOException {
      byte[] instance = reader.readBytes("instance");
      List<WrappedMessage> projection = reader.readCollection("projection", new ArrayList<>(), WrappedMessage.class);
      List<WrappedMessage> sortProjection = reader.readCollection("sortProjection", new ArrayList<>(), WrappedMessage.class);

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
   public Class<FilterResult> getJavaClass() {
      return FilterResult.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.query.remote.client.FilterResult";
   }
}
