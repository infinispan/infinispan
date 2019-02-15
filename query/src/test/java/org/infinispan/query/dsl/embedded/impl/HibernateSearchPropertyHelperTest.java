package org.infinispan.query.dsl.embedded.impl;


import static org.assertj.core.api.Assertions.assertThat;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DocumentId;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.testsupport.junit.SearchFactoryHolder;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class HibernateSearchPropertyHelperTest {

   @Rule
   public SearchFactoryHolder factoryHolder = new SearchFactoryHolder(TestEntity.class);

   private HibernateSearchPropertyHelper propertyHelper;

   @Before
   public void setup() {
      propertyHelper = new HibernateSearchPropertyHelper(factoryHolder.getSearchFactory(), new ReflectionEntityNamesResolver(null),
            HibernateSearchPropertyHelperTest.class.getClassLoader());
   }

   private Object convertToPropertyType(Class<?> type, String propertyName, String value) {
      return propertyHelper.convertToPropertyType(type, new String[]{propertyName}, value);
   }

   @Test
   public void testConvertIdProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "id", "42")).isEqualTo("42");
   }

   @Test
   public void testConvertStringProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "name", "42")).isEqualTo("42");
   }

   @Test
   public void testConvertIntProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "i", "42")).isEqualTo(42);
   }

   @Test
   public void testConvertLongProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "l", "42")).isEqualTo(42L);
   }

   @Test
   public void testConvertFloatProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "f", "42.0")).isEqualTo(42.0F);
   }

   @Test
   public void testConvertDoubleProperty() {
      assertThat(convertToPropertyType(TestEntity.class, "d", "42.0")).isEqualTo(42.0D);
   }

   @Test
   public void testRecognizeAnalyzedField() {
      assertThat(propertyHelper.getIndexedFieldProvider().get(TestEntity.class).isAnalyzed(new String[]{"description"})).isTrue();
   }

   @Test
   public void testRecognizeUnanalyzedField() {
      assertThat(propertyHelper.getIndexedFieldProvider().get(TestEntity.class).isAnalyzed(new String[]{"i"})).isFalse();
   }

   @Indexed
   static class TestEntity {

      @DocumentId
      public String id;

      @Field(analyze = Analyze.NO)
      public String name;

      @Field(analyze = Analyze.YES)
      public String description;

      @Field(analyze = Analyze.NO)
      @NumericField
      public int i;

      @Field(analyze = Analyze.NO)
      @NumericField
      public long l;

      @Field(analyze = Analyze.NO)
      @NumericField
      public float f;

      @Field(analyze = Analyze.NO)
      @NumericField
      public double d;
   }
}
