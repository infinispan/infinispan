package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

      assertNotNull(result.getQuery());

      assertEquals("org.infinispan.objectfilter.test.model.Person", result.getTargetEntityName());
      assertEquals(org.infinispan.objectfilter.test.model.Person.class, result.getTargetEntityMetadata());

      assertNotNull(result.getProjections());

      assertEquals(0, result.getProjections().size());

      assertNotNull(result.getSortFields());

      assertEquals(0, result.getSortFields().size());
   }
}
