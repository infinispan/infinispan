package org.infinispan.objectfilter.impl.syntax;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public class BooleShannonExpansionTest {

   private final EntityNamesResolver entityNamesResolver = new ReflectionEntityNamesResolver(null);
   private final ReflectionPropertyHelper propertyHelper = new ReflectionPropertyHelper(entityNamesResolver);
   private final QueryParser queryParser = new QueryParser();
   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   /**
    * @param jpaQuery        the input JPA query to parse and expand
    * @param expectedExprStr the expected 'toString()' of the output AST
    * @param expectedJpa     the expected equivalent JPA of the AST
    */
   private void assertExpectedTree(String jpaQuery, String expectedExprStr, String expectedJpa) {
      FilterParsingResult<Class<?>> parsingResult = queryParser.parseQuery(jpaQuery, FilterProcessingChain.build(entityNamesResolver, propertyHelper, null));
      BooleanExpr expr = booleanFilterNormalizer.normalize(parsingResult.getQuery());

      BooleShannonExpansion booleShannonExpansion = new BooleShannonExpansion(new BooleShannonExpansion.IndexedFieldProvider() {
         @Override
         public boolean isIndexed(List<String> propertyPath) {
            String last = propertyPath.get(propertyPath.size() - 1);
            return !last.equals("number") && !last.equals("license");
         }
      });

      expr = booleShannonExpansion.expand(expr);
      String jpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), expr, parsingResult.getSortFields());
      assertEquals(expectedExprStr, expr.toString());
      assertEquals(expectedJpa, jpaOut);
   }

   @Test
   public void testExpansionNotNeeded() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.surname = 'Adrian' or p.name = 'Nistor'",
                         "OR(EQUAL(PROP(surname), CONST(Adrian)), EQUAL(PROP(name), CONST(Nistor)))",
                         "FROM org.infinispan.objectfilter.test.model.Person WHERE (surname = \"Adrian\") OR (name = \"Nistor\")");
   }

   @Test
   public void testExpansionNotPossible() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.license = 'A' or p.name = 'Nistor'",
                         "CONST_TRUE",
                         "FROM org.infinispan.objectfilter.test.model.Person");
   }

   @Test
   public void testExpansionNotPossible2() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.name = 'A' and p.name > 'A'",
                         "CONST_FALSE",
                         "FROM org.infinispan.objectfilter.test.model.Person WHERE FALSE");
   }

   @Test
   public void testExpansionPossible() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.phoneNumbers.number != '1234' and p.surname = 'Adrian' or p.name = 'Nistor'",
                         "OR(EQUAL(PROP(surname), CONST(Adrian)), EQUAL(PROP(name), CONST(Nistor)))",
                         "FROM org.infinispan.objectfilter.test.model.Person WHERE (surname = \"Adrian\") OR (name = \"Nistor\")");
   }
}
