package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

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
      person.setPhoneNumbers(reader.readCollection("phoneNumbers", new ArrayList<PhoneNumber>(), PhoneNumber.class));
      person.setAge(reader.readInt("age"));
      person.setFavouriteNumbers(reader.readCollection("favouriteNumbers", new ArrayList<Integer>(), Integer.class));
      person.setLicense(reader.readString("license"));
      person.setGender(reader.readObject("gender", Person.Gender.class));
      person.setLastUpdate(new Date(reader.readLong("lastUpdate")));
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
      writer.writeObject("gender", person.getGender(), Person.Gender.class);
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
