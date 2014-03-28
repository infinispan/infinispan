package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.ProtobufMatcher;
import org.infinispan.objectfilter.test.model.Address;
import org.infinispan.objectfilter.test.model.AddressMarshaller;
import org.infinispan.objectfilter.test.model.Person;
import org.infinispan.objectfilter.test.model.PersonMarshaller;
import org.infinispan.objectfilter.test.model.PhoneNumber;
import org.infinispan.objectfilter.test.model.PhoneNumberMarshaller;
import org.infinispan.protostream.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;

import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherTest extends AbstractMatcherTest {

   private static final String PROTOBUF_RES = "/org/infinispan/objectfilter/test/model/test_model.protobin";

   private SerializationContext serCtx;

   @Before
   public void setUp() throws Exception {
      serCtx = ProtobufUtil.newSerializationContext(new ConfigurationBuilder().build());
      serCtx.registerProtofile(PROTOBUF_RES);
      serCtx.registerMarshaller(new AddressMarshaller());
      serCtx.registerMarshaller(new PhoneNumberMarshaller());
      serCtx.registerMarshaller(new PersonMarshaller());
   }

   @Override
   protected byte[] createPerson() throws Exception {
      Person person = new Person();
      person.setName("John");
      person.setSurname("Batman");

      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("SW12345");
      person.setAddress(address);

      PhoneNumber phoneNumber1 = new PhoneNumber();
      phoneNumber1.setNumber("0040888888");
      PhoneNumber phoneNumber2 = new PhoneNumber();
      phoneNumber2.setNumber("004012345");
      person.setPhoneNumbers(Arrays.asList(phoneNumber1, phoneNumber2));

      return ProtobufUtil.toWrappedByteArray(serCtx, person);
   }

   @Override
   protected ProtobufMatcher createMatcher() {
      return new ProtobufMatcher(serCtx);
   }
}
