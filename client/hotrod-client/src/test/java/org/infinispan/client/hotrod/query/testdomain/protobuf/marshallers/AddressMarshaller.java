package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressMarshaller implements MessageMarshaller<AddressPB> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.User.Address";
   }

   @Override
   public Class<AddressPB> getJavaClass() {
      return AddressPB.class;
   }

   @Override
   public AddressPB readFrom(ProtoStreamReader reader) throws IOException {
      String street = reader.readString("street");
      String postCode = reader.readString("postCode");
      int number = reader.readInt("number");

      AddressPB address = new AddressPB();
      address.setStreet(street);
      address.setPostCode(postCode);
      address.setNumber(number);
      return address;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AddressPB address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
      writer.writeInt("number", address.getNumber());
   }
}
