package org.infinispan.all.remote.sample.marshallers;

import org.infinispan.all.remote.sample.classes.Address;
import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
/**
 * @author anistor@redhat.com
 */
public class AddressMarshaller implements MessageMarshaller<Address> {
   @Override
   public String getTypeName() {
      return "sample_bank_account.User.Address";
   }
   @Override
   public Class<? extends Address> getJavaClass() {
      return Address.class;
   }
   @Override
   public Address readFrom(ProtoStreamReader reader) throws IOException {
      String street = reader.readString("street");
      String postCode = reader.readString("postCode");
      Address address = new Address();
      address.setStreet(street);
      address.setPostCode(postCode);
      return address;
   }
   @Override
   public void writeTo(ProtoStreamWriter writer, Address address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
   }
}
