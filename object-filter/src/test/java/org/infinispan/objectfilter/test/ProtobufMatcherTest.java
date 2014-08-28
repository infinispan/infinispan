package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.test.model.MarshallerRegistration;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherTest extends AbstractMatcherTest {

   private SerializationContext serCtx;

   @Before
   public void setUp() throws Exception {
      serCtx = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());
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
}
