package org.infinispan.query.objectfilter.test.perf;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.objectfilter.Matcher;
import org.infinispan.query.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.objectfilter.test.model.TestDomainSCI;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "query.objectfilter.test.perf.ProtobufPerfTest", groups = "profiling")
public class ProtobufPerfTest extends PerfTest {

   private SerializationContext serCtx;

   @BeforeClass
   public void setUp() {
      serCtx = ProtobufUtil.newSerializationContext();
      TestDomainSCI.INSTANCE.registerSchema(serCtx);
      TestDomainSCI.INSTANCE.registerMarshallers(serCtx);
   }

   protected Matcher createMatcher() {
      return new ProtobufMatcher(serCtx, null);
   }

   protected Object createPerson1() throws Exception {
      return ProtobufUtil.toWrappedByteArray(serCtx, super.createPerson1());
   }
}
