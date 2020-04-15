package org.infinispan.query.dsl.embedded.impl;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.DocumentId;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.helper.SearchMappingHelper;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
import org.junit.Before;
import org.junit.Test;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class HibernateSearchPropertyHelperTest {

   private HibernateSearchPropertyHelper propertyHelper;

   @Before
   public void setup() {
      SearchMappingHolder mappingHolder = mock(SearchMappingHolder.class);
      when(mappingHolder.getSearchMapping()).thenReturn(
            SearchMappingHelper.createSearchMappingForTests(TestEntity.class));
      propertyHelper = new HibernateSearchPropertyHelper(mappingHolder, new ReflectionEntityNamesResolver(null));
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
   public static class TestEntity {

      public String id;

      public String name;

      public String description;

      public int i;

      public long l;

      public float f;

      public double d;

      @DocumentId
      public String getId() {
         return id;
      }

      @Field(analyze = Analyze.NO)
      public String getName() {
         return name;
      }

      @Field(analyze = Analyze.YES)
      public String getDescription() {
         return description;
      }

      @Field(analyze = Analyze.NO)
      public int getI() {
         return i;
      }

      @Field(analyze = Analyze.NO)
      public long getL() {
         return l;
      }

      @Field(analyze = Analyze.NO)
      public float getF() {
         return f;
      }

      @Field(analyze = Analyze.NO)
      public double getD() {
         return d;
      }
   }
}
