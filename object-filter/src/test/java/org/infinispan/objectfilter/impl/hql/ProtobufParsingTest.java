package org.infinispan.objectfilter.impl.hql;

import org.infinispan.objectfilter.test.model.MarshallerRegistration;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.Descriptor;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufParsingTest extends AbstractParsingTest<Descriptor> {

   public ProtobufParsingTest() throws IOException {
      super(createPropertyHelper());
   }

   private static ObjectPropertyHelper<Descriptor> createPropertyHelper() throws IOException {
      SerializationContext serCtx = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());
      MarshallerRegistration.registerMarshallers(serCtx);
      return new ProtobufPropertyHelper(new ProtobufEntityNamesResolver(serCtx), serCtx);
   }

   @Test
   public void testParsingResult() throws Exception {
      String jpaQuery = "from org.infinispan.objectfilter.test.model.Person p where p.name is not null";
      FilterParsingResult<Descriptor> result = parser.parse(jpaQuery, propertyHelper);

      assertNotNull(result.getWhereClause());

      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityMetadata().getFullName());

      assertNull(result.getProjectedPaths());
      assertNull(result.getSortFields());
   }

   @Test
   @Override
   public void testInvalidDateLiteral() throws Exception {
      // protobuf does not have a Date type, but keep this empty test here just as a reminder
   }
}
