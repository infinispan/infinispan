package org.infinispan.protostream.sampledomain.marshallers;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.sampledomain.Address;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class AddressMarshaller implements MessageMarshaller<Address> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.User.Address";
   }

   @Override
   public Class<Address> getJavaClass() {
      return Address.class;
   }

   @Override
   public Address readFrom(ProtoStreamReader reader) throws IOException {
      String street = reader.readString("street");
      String postCode = reader.readString("postCode");
      int number = reader.readInt("number");
      Boolean isCommercial = reader.readBoolean("isCommercial");

      Address address = new Address();
      address.setStreet(street);
      address.setPostCode(postCode);
      address.setNumber(number);
      address.setCommercial(isCommercial);
      return address;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Address address) throws IOException {
      writer.writeString("street", address.getStreet());
      writer.writeString("postCode", address.getPostCode());
      writer.writeInt("number", address.getNumber());
      writer.writeBoolean("isCommercial", address.isCommercial());
   }
}
