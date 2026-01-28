package org.infinispan.query.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.infinispan.query.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class AbstractParsingTest<TypeMetadata> {

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   protected AbstractParsingTest(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   @Test
   public void testWhereTautology() {
      String queryString = "FROM org.infinispan.query.objectfilter.test.model.Person WHERE true";
      IckleParsingResult<TypeMetadata> result = IckleQueryStringParser.parse(queryString, propertyHelper);
      assertEquals(ConstantBooleanExpr.TRUE, result.getWhereClause());
      assertNull(result.getHavingClause());
   }

   @Test
   public void testWhereContradiction() {
      String queryString = "FROM org.infinispan.query.objectfilter.test.model.Person WHERE false";
      IckleParsingResult<TypeMetadata> result = IckleQueryStringParser.parse(queryString, propertyHelper);
      assertEquals(ConstantBooleanExpr.FALSE, result.getWhereClause());
      assertNull(result.getHavingClause());
   }

   @Test
   public void testRaiseExceptionDueToUnconsumedTokens() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028526");
      String queryString = "FROM IndexedEntity u WHERE u.name = 'John' blah blah blah";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidNumericLiteral() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028505");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where age = 'xyz'";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidDateLiteral() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028506");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where lastUpdate = '20140101zzzzzzzz'";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidEnumLiteral() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028508");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where gender = 'SomeUndefinedValue'";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidBooleanLiteral() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028507");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where deleted = 'maybe'";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnEmbeddedEntity() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028504");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where address = 5";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnCollectionOfEmbeddedEntity() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028504");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where phoneNumbers = 5";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testFullTextQueryNotAccepted() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028521");
      String queryString = "from org.infinispan.query.objectfilter.test.model.Person where name : 'Joe'";
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }

   @Test
   public void testSpatial() {
      //expectedException.expect(ParsingException.class);
      //expectedException.expectMessage("ISPN028534");
      String queryString = "FROM org.infinispan.query.objectfilter.test.model.Person p where p.location within circle(44, 55, 66)";

      // Must observe a parsing exception caused by spatial queries referencing a non-spatial property
      IckleQueryStringParser.parse(queryString, propertyHelper);
   }
}
