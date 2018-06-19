package org.infinispan.objectfilter.test.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.infinispan.protostream.MessageMarshaller;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PersonMarshaller implements MessageMarshaller<Person> {

   @Override
   public Person readFrom(ProtoStreamReader reader) throws IOException {
      Person person = new Person();
      person.setId(reader.readInt("id"));
      person.setName(reader.readString("name"));
      person.setSurname(reader.readString("surname"));
      person.setAddress(reader.readObject("address", Address.class));
      person.setPhoneNumbers(reader.readCollection("phoneNumbers", new ArrayList<>(), PhoneNumber.class));
      person.setAge(reader.readInt("age"));
      person.setFavouriteNumbers(reader.readCollection("favouriteNumbers", new ArrayList<>(), Integer.class));
      person.setLicense(reader.readString("license"));
      person.setGender(reader.readEnum("gender", Person.Gender.class));
      Long lastUpdate = reader.readLong("lastUpdate");
      if (lastUpdate != null) {
         person.setLastUpdate(new Date(lastUpdate));
      }
      person.setDeleted(reader.readBoolean("deleted"));
      return person;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Person person) throws IOException {
      writer.writeInt("id", person.getId());
      writer.writeString("name", person.getName());
      writer.writeString("surname", person.getSurname());
      writer.writeObject("address", person.getAddress(), Address.class);
      writer.writeCollection("phoneNumbers", person.getPhoneNumbers(), PhoneNumber.class);
      writer.writeInt("age", person.getAge());
      writer.writeCollection("favouriteNumbers", person.getFavouriteNumbers(), Integer.class);
      writer.writeString("license", person.getLicense());
      writer.writeEnum("gender", person.getGender());
      if (person.getLastUpdate() != null) {
         writer.writeLong("lastUpdate", person.getLastUpdate().getTime());
      }
      writer.writeBoolean("deleted", person.isDeleted());
   }

   @Override
   public Class<Person> getJavaClass() {
      return Person.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.objectfilter.test.model.Person";
   }
}
