package org.infinispan.query.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.objectfilter.test.model.TestDomainSCI;
import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ProtobufParsingTest extends AbstractParsingTest<Descriptor> {

   public ProtobufParsingTest() throws IOException {
      super(createPropertyHelper());
   }

   private static ObjectPropertyHelper<Descriptor> createPropertyHelper() throws IOException {
      SerializationContext serCtx = ProtobufUtil.newSerializationContext();
      TestDomainSCI.INSTANCE.registerSchema(serCtx);
      TestDomainSCI.INSTANCE.registerMarshallers(serCtx);
      return new ProtobufPropertyHelper(serCtx, null);
   }

   @Test
   public void testParsingResult() {
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person p where p.name is not null";
      IckleParsingResult<Descriptor> result = IckleParser.parse(queryString, propertyHelper);

      assertNotNull(result.getWhereClause());

      assertEquals("org.infinispan.query.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals("org.infinispan.query.objectfilter.test.model.Person", result.getTargetEntityMetadata().getFullName());

      assertNull(result.getProjectedPaths());
      assertNull(result.getSortFields());
   }

   @Test
   @Override
   public void testInvalidDateLiteral() {
      // protobuf does not have a Date type, but keep this empty test here just as a reminder
   }
}
