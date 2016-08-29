package org.infinispan.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionParsingTest extends AbstractParsingTest<Class<?>> {

   public ReflectionParsingTest() {
      super(createPropertyHelper());
   }

   private static ObjectPropertyHelper<Class<?>> createPropertyHelper() {
      return new ReflectionPropertyHelper(new ReflectionEntityNamesResolver(null));
   }

   @Test
   public void testParsingResult() throws Exception {
      String queryString = "from org.infinispan.objectfilter.test.model.Person p where p.name is not null";
      FilterParsingResult<Class<?>> result = IckleParser.parse(queryString, propertyHelper);

      assertNotNull(result.getWhereClause());

      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals(org.infinispan.objectfilter.test.model.Person.class, result.getTargetEntityMetadata());

      assertNull(result.getProjectedPaths());
      assertNull(result.getSortFields());
   }
}
