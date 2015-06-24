package org.infinispan.objectfilter.impl.syntax;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class BooleanFilterNormalizerTest {

   private final EntityNamesResolver entityNamesResolver = new ReflectionEntityNamesResolver(null);
   private final ReflectionPropertyHelper propertyHelper = new ReflectionPropertyHelper(entityNamesResolver);
   private final QueryParser queryParser = new QueryParser();
   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   private void assertExpectedTree(String jpaQuery, String expectedExprStr) {
      FilterParsingResult<Class<?>> parsingResult = queryParser.parseQuery(jpaQuery, FilterProcessingChain.build(entityNamesResolver, propertyHelper, null));
      BooleanExpr expr = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      assertEquals(expectedExprStr, expr.toString());
   }

   @Test
   public void testRepeatedPredicateDuplication1() throws Exception {
      // predicates on repeated attributes do not get optimized
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number = '1234' and p.phoneNumbers.number = '1234'",
                         "AND(EQUAL(PROP(phoneNumbers,number*), CONST(1234)), EQUAL(PROP(phoneNumbers,number*), CONST(1234)))");
   }

   @Test
   public void testPredicateDuplication1() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name = 'John' or name = 'John'",
                         "EQUAL(PROP(name), CONST(John))");
   }

   @Test
   public void testPredicateDuplication2() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name = 'John' and name = 'John'",
                         "EQUAL(PROP(name), CONST(John))");
   }

   @Test
   public void testPredicateDuplication3() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name != 'Johnny' or name != 'Johnny'",
                         "NOT_EQUAL(PROP(name), CONST(Johnny))");
   }

   @Test
   public void testPredicateDuplication4() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name != 'Johnny' and name != 'Johnny'",
                         "NOT_EQUAL(PROP(name), CONST(Johnny))");
   }

   @Test
   public void testSimpleTautology1() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where p.name = 'Noone' or not(name = 'Noone')",
                         "CONST_TRUE");
   }

   @Test
   public void testSimpleTautology2() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where p.name = 'Noone' or name != 'Noone'",
                         "CONST_TRUE");
   }

   @Test
   public void testSimpleTautology3() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where p.name > 'Noone' or name <= 'Noone'",
                         "CONST_TRUE");
   }

   @Test
   public void testSimpleContradiction1() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name = 'John' and not(name = 'John')",
                         "CONST_FALSE");
   }

   @Test
   public void testSimpleContradiction2() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name = 'John' and name != 'John'",
                         "CONST_FALSE");
   }

   @Test
   public void testSimpleContradiction3() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where name > 'John' and name <= 'John'",
                         "CONST_FALSE");
   }

   @Test
   public void testTautology() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where (name = 'Noone' and name = 'Noone') or (name != 'Noone' and name != 'Noone')",
                         "CONST_TRUE");
   }

   @Test
   public void testRepeatedIntervalOverlap1() throws Exception {
      // predicates on repeated attributes do not get optimized
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person p where p.phoneNumbers.number <= '4567' and p.phoneNumbers.number <= '5678'",
                         "AND(LESS_OR_EQUAL(PROP(phoneNumbers,number*), CONST(4567)), LESS_OR_EQUAL(PROP(phoneNumbers,number*), CONST(5678)))");
   }

   @Test
   public void testIntervalOverlap() throws Exception {
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age != 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age <= 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age >= 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age > 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age <= 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age >= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age > 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age != 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age <= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age >= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age > 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age != 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age < 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age >= 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age > 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age != 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age <= 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age > 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age != 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age <= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age >= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age > 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age != 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age < 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age > 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age = 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age < 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age <= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age >= 20", "OR(NOT_EQUAL(PROP(age), CONST(20)), GREATER_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age > 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age = 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age <= 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age >= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age > 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age = 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age != 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age < 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age >= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age > 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age = 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age != 20", "OR(GREATER_OR_EQUAL(PROP(age), CONST(20)), NOT_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age < 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age <= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age > 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age = 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age < 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age <= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age >= 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age > 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age = 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age != 30", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age < 30", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age <= 30", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age >= 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 and age > 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age = 30", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age != 30", "AND(NOT_EQUAL(PROP(age), CONST(20)), NOT_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age < 30", "AND(NOT_EQUAL(PROP(age), CONST(20)), LESS(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age <= 30", "AND(NOT_EQUAL(PROP(age), CONST(20)), LESS_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age >= 30", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 and age > 30", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age = 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age != 30", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age < 30", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age <= 30", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age >= 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 and age > 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age = 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age != 30", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age < 30", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age <= 30", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age >= 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 and age > 30", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age = 30", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age != 30", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age < 30", "AND(GREATER_OR_EQUAL(PROP(age), CONST(20)), LESS(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age <= 30", "AND(GREATER_OR_EQUAL(PROP(age), CONST(20)), LESS_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age >= 30", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 and age > 30", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age = 30", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age != 30", "AND(GREATER(PROP(age), CONST(20)), NOT_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age < 30", "AND(GREATER(PROP(age), CONST(20)), LESS(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age <= 30", "AND(GREATER(PROP(age), CONST(20)), LESS_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age >= 30", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 and age > 30", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age = 30", "OR(EQUAL(PROP(age), CONST(20)), EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age != 30", "OR(EQUAL(PROP(age), CONST(20)), NOT_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age < 30", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age <= 30", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age >= 30", "OR(EQUAL(PROP(age), CONST(20)), GREATER_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 20 or age > 30", "OR(EQUAL(PROP(age), CONST(20)), GREATER(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age = 30", "OR(NOT_EQUAL(PROP(age), CONST(20)), EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age != 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age < 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age <= 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age >= 30", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 20 or age > 30", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age = 30", "OR(LESS(PROP(age), CONST(20)), EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age != 30", "NOT_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age < 30", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age <= 30", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age >= 30", "OR(LESS(PROP(age), CONST(20)), GREATER_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 20 or age > 30", "OR(LESS(PROP(age), CONST(20)), GREATER(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age = 30", "OR(LESS_OR_EQUAL(PROP(age), CONST(20)), EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age != 30", "NOT_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age < 30", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age <= 30", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age >= 30", "OR(LESS_OR_EQUAL(PROP(age), CONST(20)), GREATER_OR_EQUAL(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 20 or age > 30", "OR(LESS_OR_EQUAL(PROP(age), CONST(20)), GREATER(PROP(age), CONST(30)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age = 30", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age != 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age < 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age <= 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age >= 30", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 20 or age > 30", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age = 30", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age != 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age < 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age <= 30", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age >= 30", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 20 or age > 30", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age != 20", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age <= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age >= 20", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 and age > 20", "EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age != 20", "AND(NOT_EQUAL(PROP(age), CONST(30)), NOT_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age >= 20", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 and age > 20", "AND(NOT_EQUAL(PROP(age), CONST(30)), GREATER(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age != 20", "AND(LESS(PROP(age), CONST(30)), NOT_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age >= 20", "AND(LESS(PROP(age), CONST(30)), GREATER_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 and age > 20", "AND(LESS(PROP(age), CONST(30)), GREATER(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age = 20", "EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age != 20", "AND(LESS_OR_EQUAL(PROP(age), CONST(30)), NOT_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age < 20", "LESS(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age >= 20", "AND(LESS_OR_EQUAL(PROP(age), CONST(30)), GREATER_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 and age > 20", "AND(LESS_OR_EQUAL(PROP(age), CONST(30)), GREATER(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age != 20", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age <= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 and age > 20", "GREATER_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age = 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age != 20", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age < 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age <= 20", "CONST_FALSE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age >= 20", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 and age > 20", "GREATER(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age = 20", "OR(EQUAL(PROP(age), CONST(30)), EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age != 20", "OR(EQUAL(PROP(age), CONST(30)), NOT_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age < 20", "OR(EQUAL(PROP(age), CONST(30)), LESS(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age <= 20", "OR(EQUAL(PROP(age), CONST(30)), LESS_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age = 30 or age > 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age = 20", "OR(NOT_EQUAL(PROP(age), CONST(30)), EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age != 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age < 20", "NOT_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age <= 20", "NOT_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age >= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age != 30 or age > 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age = 20", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age != 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age < 20", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age <= 20", "LESS(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age >= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age < 30 or age > 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age = 20", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age != 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age < 20", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age <= 20", "LESS_OR_EQUAL(PROP(age), CONST(30))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age >= 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age <= 30 or age > 20", "CONST_TRUE");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age = 20", "OR(GREATER_OR_EQUAL(PROP(age), CONST(30)), EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age < 20", "OR(GREATER_OR_EQUAL(PROP(age), CONST(30)), LESS(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age <= 20", "OR(GREATER_OR_EQUAL(PROP(age), CONST(30)), LESS_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age >= 30 or age > 20", "GREATER(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age = 20", "OR(GREATER(PROP(age), CONST(30)), EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age != 20", "NOT_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age < 20", "OR(GREATER(PROP(age), CONST(30)), LESS(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age <= 20", "OR(GREATER(PROP(age), CONST(30)), LESS_OR_EQUAL(PROP(age), CONST(20)))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age >= 20", "GREATER_OR_EQUAL(PROP(age), CONST(20))");
      assertExpectedTree("from org.infinispan.objectfilter.test.model.Person where age > 30 or age > 20", "GREATER(PROP(age), CONST(20))");
   }
}
