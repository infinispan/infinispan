package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PhoneNumberMarshaller implements MessageMarshaller<PhoneNumber> {

   @Override
   public PhoneNumber readFrom(ProtoStreamReader reader) throws IOException {
      PhoneNumber phoneNumber = new PhoneNumber();
      phoneNumber.setNumber(reader.readString("number"));
      return phoneNumber;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, PhoneNumber phoneNumber) throws IOException {
      writer.writeString("number", phoneNumber.getNumber());
   }

   @Override
   public Class<? extends PhoneNumber> getJavaClass() {
      return PhoneNumber.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.objectfilter.test.model.PhoneNumber";
   }
}
