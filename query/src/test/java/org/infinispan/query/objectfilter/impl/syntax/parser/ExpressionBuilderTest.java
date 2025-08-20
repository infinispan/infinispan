package org.infinispan.query.objectfilter.impl.syntax.parser;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.ComparisonExpr;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link ExpressionBuilder}.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class ExpressionBuilderTest {

   private ExpressionBuilder<Class<?>> builder;

   @Before
   public void setup() {
      ReflectionPropertyHelper propertyHelper = new ReflectionPropertyHelper(new ReflectionEntityNamesResolver(null));
      builder = new ExpressionBuilder<>(propertyHelper);
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
   public void testNestedExpressionQuery() {
      TestPropertyHelper propertyHelper = new TestPropertyHelper(new ReflectionEntityNamesResolver(null), true);
      builder = new ExpressionBuilder<>(propertyHelper);
      builder.setEntityType(TestEntity.class);
      builder.pushAnd();
      builder.addNestedComparison(PropertyPath.make("embedded.title"), ComparisonExpr.Type.EQUAL, "dummy1", "a", PropertyPath.make("embedded"));
      builder.addNestedComparison(PropertyPath.make("anotherEmbedded.title"), ComparisonExpr.Type.EQUAL, "dummy3", "b", PropertyPath.make("embedded"));
      builder.pop();
      BooleanExpr query = builder.build();

      assertEquals("AND(NESTED(EQUAL(PROP(embedded.title), CONST(\"dummy1\"))), NESTED(EQUAL(PROP(anotherEmbedded.title), CONST(\"dummy3\"))))", query.toString());
   }

   @Test
   public void testNestedExpressionOnNonEmbeddedFieldQuery() {
      TestPropertyHelper propertyHelper = new TestPropertyHelper(new ReflectionEntityNamesResolver(null), false);
      builder = new ExpressionBuilder<>(propertyHelper);
      builder.setEntityType(TestEntity.class);
      builder.pushAnd();
      builder.addNestedComparison(PropertyPath.make("embedded.title"), ComparisonExpr.Type.EQUAL, "dummy1", "a", PropertyPath.make("embedded"));
      builder.addNestedComparison(PropertyPath.make("anotherEmbedded.title"), ComparisonExpr.Type.EQUAL, "dummy3", "b", PropertyPath.make("embedded"));
      builder.pop();
      BooleanExpr query = builder.build();

      assertEquals("AND(EQUAL(PROP(embedded.title), CONST(\"dummy1\")), EQUAL(PROP(anotherEmbedded.title), CONST(\"dummy3\")))", query.toString());
   }

   @Test
   public void testMultiLevelNestedExpressionQuery() {
      TestPropertyHelper propertyHelper = new TestPropertyHelper(new ReflectionEntityNamesResolver(null), true);
      builder = new ExpressionBuilder<>(propertyHelper);
      builder.setEntityType(TestEntity.class);
      builder.addNestedComparison(PropertyPath.make("embedded.anotherEmbedded.name"), ComparisonExpr.Type.EQUAL, "foo", "a", PropertyPath.make("embedded"));
      BooleanExpr query = builder.build();

      assertEquals("NESTED(EQUAL(PROP(embedded.anotherEmbedded.name), CONST(\"foo\")))", query.toString());
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
      public EmbeddedTestEntity anotherEmbedded;

      static class EmbeddedTestEntity {
         public String title;
         public DeepEmbeddedTestEntity deepEmbedded;
      }

      static class DeepEmbeddedTestEntity {
         public String name;
      }
   }

   static class TestPropertyHelper extends ReflectionPropertyHelper{

      boolean isNested;

      public TestPropertyHelper(EntityNameResolver<Class<?>> entityNameResolver, boolean isNested) {
         super(entityNameResolver);
         this.isNested = isNested;
      }

      @Override
      public boolean isNestedIndexStructure(Class<?> entityType, String[] propertyPath) {
         return isNested;
      }
   }
}
