package org.infinispan.client.hotrod.protostream.domain.marshallers;

import org.infinispan.client.hotrod.protostream.domain.Address;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 */
public class AddressMarshaller implements MessageMarshaller<Address> {

   @Override
   public String getFullName() {
      return "sample_bank_account.User.Address";
   }

   @Override
   public Address readFrom(ProtobufReader reader) throws IOException {
      String street = reader.readString("street");
      String postCode = reader.readString("postCode");

      Address address = new Address();
      address.setStreet(street);
      address.setPostCode(postCode);
      return address;
   }

   @Override
   public void writeTo(ProtobufWriter writer, Address address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
   }
}
