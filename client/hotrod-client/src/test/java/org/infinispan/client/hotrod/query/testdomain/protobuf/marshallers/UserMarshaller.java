package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class UserMarshaller implements MessageMarshaller<UserPB> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.User";
   }

   @Override
   public Class<UserPB> getJavaClass() {
      return UserPB.class;
   }

   @Override
   public UserPB readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      Set<Integer> accountIds = reader.readCollection("accountIds", new HashSet<Integer>(), Integer.class);

      // Read them out of order. It still works but logs a warning!
      String surname = reader.readString("surname");
      String name = reader.readString("name");

      List<Address> addresses = reader.readCollection("addresses", new ArrayList<Address>(), AddressPB.class);

      Integer age = reader.readInt("age");
      User.Gender gender = reader.readObject("gender", User.Gender.class);
      String notes = reader.readString("notes");

      UserPB user = new UserPB();
      user.setId(id);
      user.setAccountIds(accountIds);
      user.setName(name);
      user.setSurname(surname);
      user.setAge(age);
      user.setGender(gender);
      user.setAddresses(addresses);
      user.setNotes(notes);
      return user;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, UserPB user) throws IOException {
      writer.writeInt("id", user.getId());
      writer.writeCollection("accountIds", user.getAccountIds(), Integer.class);
      writer.writeString("name", user.getName());
      writer.writeString("surname", user.getSurname());
      writer.writeCollection("addresses", user.getAddresses(), AddressPB.class);
      writer.writeInt("age", user.getAge());
      writer.writeObject("gender", user.getGender(), User.Gender.class);
      writer.writeString("notes", user.getNotes());
   }
}
