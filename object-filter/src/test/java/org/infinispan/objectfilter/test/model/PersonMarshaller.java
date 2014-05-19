package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.MessageMarshaller;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class PersonMarshaller implements MessageMarshaller<Person> {

   @Override
   public Person readFrom(ProtoStreamReader reader) throws IOException {
      Person person = new Person();
      person.setName(reader.readString("name"));
      person.setSurname(reader.readString("surname"));
      person.setAddress(reader.readObject("address", Address.class));
      person.setPhoneNumbers(reader.readCollection("phoneNumbers", new ArrayList<PhoneNumber>(), PhoneNumber.class));
      person.setAge(reader.readInt("age"));
      person.setLicense(reader.readString("license"));
      person.setGender(reader.readObject("gender", Person.Gender.class));
      return person;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Person person) throws IOException {
      writer.writeString("name", person.getName());
      writer.writeString("surname", person.getSurname());
      writer.writeObject("address", person.getAddress(), Address.class);
      writer.writeCollection("phoneNumbers", person.getPhoneNumbers(), PhoneNumber.class);
      writer.writeInt("age", person.getAge());
      writer.writeString("license", person.getLicense());
      writer.writeObject("gender", person.getGender(), Person.Gender.class);
   }

   @Override
   public Class<? extends Person> getJavaClass() {
      return Person.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.objectfilter.test.model.Person";
   }
}
