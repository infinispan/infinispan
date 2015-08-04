package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionParsingTest extends AbstractParsingTest {

   @Override
   protected FilterProcessingChain<Class<?>> createFilterProcessingChain() throws Exception {
      EntityNamesResolver entityNamesResolver = new ReflectionEntityNamesResolver(null);
      ReflectionPropertyHelper reflectionPropertyHelper = new ReflectionPropertyHelper(entityNamesResolver);
      return FilterProcessingChain.build(entityNamesResolver, reflectionPropertyHelper, null);
   }

   @Test
   public void testParsingResult() throws Exception {
      String jpaQuery = "from org.infinispan.objectfilter.test.model.Person p where p.name is not null";
      FilterParsingResult<Class<?>> result = queryParser.parseQuery(jpaQuery, createFilterProcessingChain());

      assertNotNull(result.getWhereClause());

      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals(org.infinispan.objectfilter.test.model.Person.class, result.getTargetEntityMetadata());

      assertNull(result.getProjectedPaths());
      assertNull(result.getSortFields());
   }
}
