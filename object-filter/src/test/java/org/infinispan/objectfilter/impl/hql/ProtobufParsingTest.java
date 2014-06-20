package org.infinispan.objectfilter.impl.hql;

import com.google.protobuf.Descriptors;
import org.infinispan.objectfilter.test.model.MarshallerRegistration;
import org.infinispan.protostream.ConfigurationBuilder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufParsingTest extends AbstractParsingTest {

   @Override
   protected FilterProcessingChain<Descriptors.Descriptor> createFilterProcessingChain() throws IOException, Descriptors.DescriptorValidationException {
      SerializationContext serCtx = ProtobufUtil.newSerializationContext(new ConfigurationBuilder().build());
      MarshallerRegistration.registerMarshallers(serCtx);
      return FilterProcessingChain.build(new ProtobufPropertyHelper(serCtx), null);
   }

   @Test
   public void testParsingResult() throws Exception {
      String jpaQuery = "from org.infinispan.objectfilter.test.model.Person p where p.name is not null";
      FilterParsingResult<Descriptors.Descriptor> result = queryParser.parseQuery(jpaQuery, createFilterProcessingChain());

      assertNotNull(result.getQuery());

      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityMetadata().getFullName());

      assertNotNull(result.getProjections());

      assertEquals(0, result.getProjections().size());

      assertNotNull(result.getSortFields());

      assertEquals(0, result.getSortFields().size());
   }

   @Ignore("protobuf does not have a Date type")
   @Test
   @Override
   public void testInvalidDateLiteral() throws Exception {
      // keep this test here just as a reminder
      super.testInvalidDateLiteral();
   }
}
