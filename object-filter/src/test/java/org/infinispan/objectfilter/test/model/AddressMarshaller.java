package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressMarshaller implements MessageMarshaller<Address> {

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

   @Override
   public Class<Address> getJavaClass() {
      return Address.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.objectfilter.test.model.Address";
   }
}
