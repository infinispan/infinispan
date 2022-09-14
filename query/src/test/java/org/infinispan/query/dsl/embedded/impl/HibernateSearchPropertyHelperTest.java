package org.infinispan.query.dsl.embedded.impl;


import static org.assertj.core.api.Assertions.assertThat;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.DocumentId;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.query.helper.SearchMappingHelper;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class HibernateSearchPropertyHelperTest extends SingleCacheManagerTest {

   private SearchMapping searchMapping;
   private HibernateSearchPropertyHelper propertyHelper;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();

      GlobalComponentRegistry componentRegistry = cacheManager.getGlobalComponentRegistry();
      BlockingManager blockingManager = componentRegistry.getComponent(BlockingManager.class);
      NonBlockingManager nonBlockingManager = componentRegistry.getComponent(NonBlockingManager.class);

      // the cache manager is created only to provide manager instances to the search mapping
      searchMapping = SearchMappingHelper.createSearchMappingForTests(blockingManager, nonBlockingManager, TestEntity.class);
      propertyHelper = new HibernateSearchPropertyHelper(searchMapping, new ReflectionEntityNamesResolver(null));

      return cacheManager;
   }

   @Override
   protected void teardown() {
      if (searchMapping != null) {
         searchMapping.close();
      }
      super.teardown();
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
   public void testRecognizeStoredField() {
      assertThat(propertyHelper.getIndexedFieldProvider().get(TestEntity.class).isProjectable(new String[]{"description"})).isTrue();
      assertThat(propertyHelper.getIndexedFieldProvider().get(TestEntity.class).isSortable(new String[]{"description"})).isFalse();
   }

   @Test
   public void testRecognizeUnanalyzedField() {
      assertThat(propertyHelper.getIndexedFieldProvider().get(TestEntity.class).isAnalyzed(new String[]{"i"})).isFalse();
   }

   @Indexed
   public static class TestEntity {

      public String id;

      @Basic
      public String name;

      @Text(projectable = true)
      public String description;

      public int i;

      public long l;

      public float f;

      public double d;

      // When an entity is created with Infinispan,
      // the document id is reserved to link the cache entry key to the value.
      // In this case Hibernate Search is used standalone,
      // so we need to provide explicitly the document id,
      // using the Search annotation.
      @DocumentId
      public String getId() {
         return id;
      }

      @Basic
      public int getI() {
         return i;
      }

      @Basic
      public long getL() {
         return l;
      }

      @Basic
      public float getF() {
         return f;
      }

      @Basic
      public double getD() {
         return d;
      }
   }
}
