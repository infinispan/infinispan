package org.infinispan.objectfilter.impl.syntax;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.JPQLParser;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public class BooleShannonExpansionTest {

   private final EntityNamesResolver entityNamesResolver = new ReflectionEntityNamesResolver(null);
   private final ReflectionPropertyHelper propertyHelper = new ReflectionPropertyHelper(entityNamesResolver);
   private final JPQLParser<Class<?>> parser = new JPQLParser<>(entityNamesResolver, propertyHelper);
   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   /**
    * @param jpaQuery        the input JPA query to parse and expand
    * @param expectedExprStr the expected 'toString()' of the output AST
    * @param expectedJpa     the expected equivalent JPA of the AST
    */
   private void assertExpectedTree(String jpaQuery, String expectedExprStr, String expectedJpa) {
      FilterParsingResult<Class<?>> parsingResult = parser.parse(jpaQuery);
      BooleanExpr expr = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());

      BooleShannonExpansion booleShannonExpansion = new BooleShannonExpansion(3, new BooleShannonExpansion.IndexedFieldProvider() {
         @Override
         public boolean isIndexed(String[] propertyPath) {
            String last = propertyPath[propertyPath.length - 1];
            return !"number".equals(last) && !"license".equals(last);
         }

         @Override
         public boolean isStored(String[] propertyPath) {
            return isIndexed(propertyPath);
         }
      });

      expr = booleShannonExpansion.expand(expr);
      if (expectedExprStr != null) {
         assertEquals(expectedExprStr, expr.toString());
      }
      if (expectedJpa != null) {
         String jpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), null, expr, parsingResult.getSortFields());
         assertEquals(expectedJpa, jpaOut);
      }
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
                         null);
   }

   @Test
   public void testExpansionPossible() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.phoneNumbers.number != '1234' and p.surname = 'Adrian' or p.name = 'Nistor'",
                         "OR(EQUAL(PROP(surname), CONST(Adrian)), EQUAL(PROP(name), CONST(Nistor)))",
                         "FROM org.infinispan.objectfilter.test.model.Person WHERE (surname = \"Adrian\") OR (name = \"Nistor\")");
   }

   @Test
   public void testExpansionTooBig() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where " +
                               "p.phoneNumbers.number != '1234' and p.surname = 'Adrian' or p.name = 'Nistor' and license = 'PPL'",
                         "CONST_TRUE",
                         "FROM org.infinispan.objectfilter.test.model.Person");
   }
}
