package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.test.model.MarshallerRegistration;
import org.infinispan.protostream.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherTest extends AbstractMatcherTest {

   private SerializationContext serCtx;

   @Before
   public void setUp() throws Exception {
      serCtx = ProtobufUtil.newSerializationContext(new ConfigurationBuilder().build());
      MarshallerRegistration.registerMarshallers(serCtx);
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

   @Test
   @Ignore
   @Override
   public void testCollectionIsNotNull1() throws Exception {
      //todo [anistor] this feature is currently not implemented for the protobuf case
      super.testCollectionIsNotNull1();
   }
}
