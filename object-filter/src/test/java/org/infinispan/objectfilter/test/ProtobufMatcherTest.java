package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.test.model.AddressMarshaller;
import org.infinispan.objectfilter.test.model.GenderMarshaller;
import org.infinispan.objectfilter.test.model.PersonMarshaller;
import org.infinispan.objectfilter.test.model.PhoneNumberMarshaller;
import org.infinispan.protostream.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;
import org.junit.Ignore;

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
      serCtx.registerMarshaller(new GenderMarshaller());
      serCtx.registerMarshaller(new PersonMarshaller());
   }

   @Override
   protected byte[] createPerson1() throws Exception {
      return ProtobufUtil.toWrappedByteArray(serCtx, super.createPerson1());
   }

   @Override
   protected byte[] createPerson2() throws Exception {
      return ProtobufUtil.toWrappedByteArray(serCtx, super.createPerson2());
   }

   @Override
   protected ProtobufMatcher createMatcher() {
      return new ProtobufMatcher(serCtx);
   }

   @Ignore
   public void testCollectionIsNotNull1() throws Exception {
      //todo [anistor] this feature is currently not implemented for the protobuf case
   }
}
