package org.infinispan.query.objectfilter.test;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.objectfilter.test.model.TestDomainSCI;
import org.junit.Before;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "query.objectfilter.test.ProtobufMatcherTest", groups = "functional")
public class ProtobufMatcherTest extends AbstractMatcherTest {

   private SerializationContext serCtx;

   @Before
   public void setUp() {
      serCtx = ProtobufUtil.newSerializationContext();
      TestDomainSCI.INSTANCE.register(serCtx);
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
      return new ProtobufMatcher(serCtx, null);
   }
}
