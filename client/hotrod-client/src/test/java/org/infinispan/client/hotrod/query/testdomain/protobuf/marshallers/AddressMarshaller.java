package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.protostream.MessageMarshaller;

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
      Boolean isCommercial = reader.readBoolean("isCommercial");

      AddressPB address = new AddressPB();
      address.setStreet(street);
      address.setPostCode(postCode);
      address.setNumber(number);
      address.setCommercial(isCommercial);
      return address;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AddressPB address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
      writer.writeInt("number", address.getNumber());
      writer.writeBoolean("isCommercial", address.isCommercial());
   }
}
