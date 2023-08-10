package org.infinispan.protostream.sampledomain.marshallers;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.UnknownFieldSet;
import org.infinispan.protostream.UnknownFieldSetHandler;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class UserMarshaller implements MessageMarshaller<User>, UnknownFieldSetHandler<User> {

   @Override
   public String getTypeName() {
      return "sample_bank_account.User";
   }

   @Override
   public Class<User> getJavaClass() {
      return User.class;
   }

   @Override
   public User readFrom(ProtoStreamReader reader) throws IOException {
      int id = reader.readInt("id");
      Set<Integer> accountIds = reader.readCollection("accountIds", new HashSet<>(), Integer.class);

      String name = reader.readString("name");
      String surname = reader.readString("surname");
      String salutation = reader.readString("salutation");

      List<Address> addresses = reader.readCollection("addresses", new ArrayList<>(), Address.class);

      Integer age = reader.readInt("age");
      User.Gender gender = reader.readEnum("gender", User.Gender.class);
      String notes = reader.readString("notes");
      Instant creationDate = reader.readInstant("creationDate");
      Instant passwordExpirationDate = reader.readInstant("passwordExpirationDate");

      User user = new User();
      user.setId(id);
      user.setAccountIds(accountIds);
      user.setName(name);
      user.setSurname(surname);
      user.setSalutation(salutation);
      user.setAge(age);
      user.setGender(gender);
      user.setAddresses(addresses);
      user.setNotes(notes);
      user.setCreationDate(creationDate);
      user.setPasswordExpirationDate(passwordExpirationDate);
      return user;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, User user) throws IOException {
      writer.writeInt("id", user.getId());
      writer.writeCollection("accountIds", user.getAccountIds(), Integer.class);
      writer.writeString("name", user.getName());
      writer.writeString("surname", user.getSurname());
      writer.writeString("salutation", user.getSalutation());
      writer.writeCollection("addresses", user.getAddresses(), Address.class);
      writer.writeInt("age", user.getAge());
      writer.writeEnum("gender", user.getGender());
      writer.writeString("notes", user.getNotes());
      writer.writeInstant("creationDate", user.getCreationDate());
      writer.writeInstant("passwordExpirationDate", user.getPasswordExpirationDate());
   }

   @Override
   public UnknownFieldSet getUnknownFieldSet(User user) {
      return user.getUnknownFieldSet();
   }

   @Override
   public void setUnknownFieldSet(User user, UnknownFieldSet unknownFieldSet) {
      user.setUnknownFieldSet(unknownFieldSet);
   }
}
