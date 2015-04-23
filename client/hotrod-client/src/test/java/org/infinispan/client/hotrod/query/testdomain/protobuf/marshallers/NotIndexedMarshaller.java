package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexedMarshaller implements MessageMarshaller<NotIndexed> {

   @Override
   public NotIndexed readFrom(ProtoStreamReader reader) throws IOException {
      String notIndexedField = reader.readString("notIndexedField");
      return new NotIndexed(notIndexedField);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, NotIndexed notIndexed) throws IOException {
      writer.writeString("notIndexedField", notIndexed.notIndexedField);
   }

   @Override
   public Class<? extends NotIndexed> getJavaClass() {
      return NotIndexed.class;
   }

   @Override
   public String getTypeName() {
      return "sample_bank_account.NotIndexed";
   }
}
