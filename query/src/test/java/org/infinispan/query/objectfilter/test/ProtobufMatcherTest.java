package org.infinispan.query.objectfilter.test;

import org.infinispan.query.objectfilter.impl.ProtobufMatcher;
import org.infinispan.query.objectfilter.test.model.TestDomainSCI;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Before;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufMatcherTest extends AbstractMatcherTest {

   private SerializationContext serCtx;

   private FilterQueryFactory queryFactory;

   @Before
   public void setUp() throws Exception {
      serCtx = ProtobufUtil.newSerializationContext();
      TestDomainSCI.INSTANCE.registerSchema(serCtx);
      TestDomainSCI.INSTANCE.registerMarshallers(serCtx);
      queryFactory = new FilterQueryFactory(serCtx);
   }

   @Override
   protected FilterQueryFactory createQueryFactory() {
      return queryFactory;
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
