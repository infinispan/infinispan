package org.infinispan.objectfilter.test.perf;

import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.test.model.MarshallerRegistration;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Category(Profiling.class)
@Ignore
public class ProtobufPerfTest extends PerfTest {

   private SerializationContext serCtx;

   @Before
   public void setUp() throws Exception {
      serCtx = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());
      MarshallerRegistration.registerMarshallers(serCtx);
   }

   protected Matcher createMatcher() throws Exception {
      return new ProtobufMatcher(serCtx);
   }

   protected Object createPerson1() throws Exception {
      return ProtobufUtil.toWrappedByteArray(serCtx, super.createPerson1());
   }
}
