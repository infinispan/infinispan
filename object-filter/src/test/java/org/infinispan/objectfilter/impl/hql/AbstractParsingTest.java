package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ParsingException;
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

   protected final JPQLParser<TypeMetadata> parser = new JPQLParser<>();

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   public AbstractParsingTest(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   @Test
   public void testInvalidNumericLiteral() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000407");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where age = 'xyz'";
      parser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidDateLiteral() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000408");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where lastUpdate = '20140101zzzzzzzz'";
      parser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidEnumLiteral() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000410");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where gender = 'SomeUndefinedValue'";
      parser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidBooleanLiteral() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000409");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where deleted = 'maybe'";
      parser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnEmbeddedEntity() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000406");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where address = 5";
      parser.parse(queryString, propertyHelper);
   }

   @Test
   public void testInvalidPredicateOnCollectionOfEmbeddedEntity() throws Exception {
      expectedException.expect(ParsingException.class);
      expectedException.expectMessage("ISPN000406");
      String queryString = "from org.infinispan.objectfilter.test.model.Person where phoneNumbers = 5";
      parser.parse(queryString, propertyHelper);
   }
}
