package org.infinispan.all.remote.sample.marshallers;

import java.io.IOException;

import org.infinispan.all.remote.sample.AddressPB;
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

      AddressPB address = new AddressPB();
      address.setStreet(street);
      address.setPostCode(postCode);
      return address;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, AddressPB address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
   }
}

