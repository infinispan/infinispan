package org.infinispan.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class AbstractParsingTest<TypeMetadata> {

   @Rule
   public ExpectedException expectedException = ExpectedException.none();

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   protected AbstractParsingTest(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   @Test
   public void testWhereTautology() {
      String queryString = "FROM org.infinispan.objectfilter.test.model.Person WHERE true";
      IckleParsingResult<TypeMetadata> result = IckleParser.parse(queryString, propertyHelper);
      assertEquals(ConstantBooleanExpr.TRUE, result.getWhereClause());
      assertNull(result.getHavingClause());
   }

   @Test
   public void testWhereContradiction() {
      String queryString = "FROM org.infinispan.objectfilter.test.model.Person WHERE false";
      IckleParsingResult<TypeMetadata> result = IckleParser.parse(queryString, propertyHelper);
      assertEquals(ConstantBooleanExpr.FALSE, result.getWhereClause());
      assertNull(result.getHavingClause());
   }

   @Test
   public void testRaiseExceptionDueToUnconsumedTokens() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028526");
      String queryString = "FROM IndexedEntity u WHERE u.name = 'John' blah blah blah";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidNumericLiteral() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028505");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age = 'xyz'";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidDateLiteral() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028506");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where lastUpdate = '20140101zzzzzzzz'";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidEnumLiteral() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028508");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where gender = 'SomeUndefinedValue'";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidBooleanLiteral() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028507");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where deleted = 'maybe'";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnEmbeddedEntity() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028504");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where address = 5";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnCollectionOfEmbeddedEntity() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028504");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers = 5";
      IckleParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testFullTextQueryNotAccepted() {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN028521");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where name : 'Joe'";
      IckleParser.parse(queryString, propertyHelper);
   }
}
