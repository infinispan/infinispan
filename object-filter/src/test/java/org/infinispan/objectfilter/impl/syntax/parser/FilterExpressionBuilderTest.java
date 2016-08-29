package org.infinispan.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link FilterExpressionBuilder}.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class FilterExpressionBuilderTest {

   private FilterExpressionBuilder<Class<?>> builder;

   @Before
   public void setup() {
      ReflectionPropertyHelper propertyHelper = new ReflectionPropertyHelper(new ReflectionEntityNamesResolver(null));
      builder = new FilterExpressionBuilder<>(propertyHelper);
   }

   @Test
   public void testStringEqualsQuery() {
      builder.setEntityType(TestEntity.class);
      builder.addComparison(PropertyPath.make("name"), ComparisonExpr.Type.EQUAL, "foo");
      BooleanExpr query = builder.build();

      assertEquals("EQUAL(PROP(name), CONST(\"foo\"))", query.toString());
   }

   @Test
   public void testStringEqualsQueryOnEmbeddedProperty() {
      builder.setEntityType(TestEntity.class);
      builder.addComparison(PropertyPath.make("embedded.title"), ComparisonExpr.Type.EQUAL, "bar");
      BooleanExpr query = builder.build();

      assertEquals("EQUAL(PROP(embedded.title), CONST(\"bar\"))", query.toString());
   }

   @Test
   public void testLongEqualsQuery() {
      builder.setEntityType(TestEntity.class);
      builder.addComparison(PropertyPath.make("l"), ComparisonExpr.Type.EQUAL, 10);
      BooleanExpr query = builder.build();

      assertEquals("EQUAL(PROP(l), CONST(10))", query.toString());
   }

   @Test
   public void testDoubleEqualsQuery() {
      builder.setEntityType(TestEntity.class);
      builder.addComparison(PropertyPath.make("d"), ComparisonExpr.Type.EQUAL, 10.0);
      BooleanExpr query = builder.build();

      assertEquals("EQUAL(PROP(d), CONST(10.0))", query.toString());
   }

   @Test
   public void testDateEqualsQuery() throws Exception {
      builder.setEntityType(TestEntity.class);
      builder.addComparison(PropertyPath.make("date"), ComparisonExpr.Type.EQUAL, makeDate("2016-09-25"));
      BooleanExpr query = builder.build();

      assertEquals("EQUAL(PROP(date), CONST(20160925000000000))", query.toString());
   }

   @Test
   public void testDateRangeQuery() throws Exception {
      builder.setEntityType(TestEntity.class);
      builder.addRange(PropertyPath.make("date"), makeDate("2016-8-25"), makeDate("2016-10-25"));
      BooleanExpr query = builder.build();

      assertEquals("BETWEEN(PROP(date), CONST(20160825000000000), CONST(20161025000000000))", query.toString());
   }

   @Test
   public void testIntegerRangeQuery() {
      builder.setEntityType(TestEntity.class);
      builder.addRange(PropertyPath.make("i"), 1, 10);
      BooleanExpr query = builder.build();

      assertEquals("BETWEEN(PROP(i), CONST(1), CONST(10))", query.toString());
   }

   @Test
   public void testNegationQuery() {
      builder.setEntityType(TestEntity.class);
      builder.pushNot();
      builder.addComparison(PropertyPath.make("name"), ComparisonExpr.Type.EQUAL, "foo");
      BooleanExpr query = builder.build();

      assertEquals("NOT(EQUAL(PROP(name), CONST(\"foo\")))", query.toString());
   }

   @Test
   public void testConjunctionQuery() {
      builder.setEntityType(TestEntity.class);
      builder.pushAnd();
      builder.addComparison(PropertyPath.make("name"), ComparisonExpr.Type.EQUAL, "foo");
      builder.addComparison(PropertyPath.make("i"), ComparisonExpr.Type.EQUAL, 1);
      BooleanExpr query = builder.build();

      assertEquals("AND(EQUAL(PROP(name), CONST(\"foo\")), EQUAL(PROP(i), CONST(1)))", query.toString());
   }

   @Test
   public void testDisjunctionQuery() {
      builder.setEntityType(TestEntity.class);
      builder.pushOr();
      builder.addComparison(PropertyPath.make("name"), ComparisonExpr.Type.EQUAL, "foo");
      builder.addComparison(PropertyPath.make("i"), ComparisonExpr.Type.EQUAL, 1);
      BooleanExpr query = builder.build();

      assertEquals("OR(EQUAL(PROP(name), CONST(\"foo\")), EQUAL(PROP(i), CONST(1)))", query.toString());
   }

   @Test
   public void testNestedLogicalPredicatesQuery() {
      builder.setEntityType(TestEntity.class);
      builder.pushAnd();
      builder.pushOr();
      builder.addComparison(PropertyPath.make("name"), ComparisonExpr.Type.EQUAL, "foo");
      builder.addComparison(PropertyPath.make("i"), ComparisonExpr.Type.EQUAL, 1);
      builder.pop();
      builder.addComparison(PropertyPath.make("l"), ComparisonExpr.Type.EQUAL, 10);
      BooleanExpr query = builder.build();

      assertEquals("AND(OR(EQUAL(PROP(name), CONST(\"foo\")), EQUAL(PROP(i), CONST(1))), EQUAL(PROP(l), CONST(10)))", query.toString());
   }

   private Date makeDate(String input) throws ParseException {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      return dateFormat.parse(input);
   }

   static class TestEntity {

      public String id;

      public String name;

      public Date date;

      public int i;

      public long l;

      public float f;

      public double d;

      public EmbeddedTestEntity embedded;

      static class EmbeddedTestEntity {
         public String title;
      }
   }
}
