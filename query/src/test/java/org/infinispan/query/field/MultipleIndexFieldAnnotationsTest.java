package org.infinispan.query.field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.model.Book;
import org.infinispan.query.model.Color;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.parameter.IndexFieldNameTest")
public class MultipleIndexFieldAnnotationsTest extends SingleCacheManagerTest {

   // descriptions taken are from Wikipedia https://en.wikipedia.org/wiki/*
   public static final String RED_DESCRIPTION = "Red is the color at the long wavelength end of the visible spectrum of light";
   public static final String GREEN_DESCRIPTION = "Green is the color between cyan and yellow on the visible spectrum.";
   public static final String BLUE_DESCRIPTION = "Blue is one of the three primary colours in the RYB colour model (traditional color theory), as well as in the RGB (additive) colour model.";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Color.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @BeforeMethod(alwaysRun = true)
   public void beforeMethod() {
      cache.put(1, new Color("red", RED_DESCRIPTION));
      cache.put(2, new Color("green", GREEN_DESCRIPTION));
      cache.put(3, new Color("blue", BLUE_DESCRIPTION));
   }

   @Test
   public void testTargetingDifferentIndexFields() {
      QueryFactory factory = Search.getQueryFactory(cache);
      Query<Book> query = factory.create("from org.infinispan.query.model.Color where name = 'red'");
      QueryResult<Book> result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("red");

      query = factory.create(String.format("from %s where desc1 = '%s'", Color.class.getName(), RED_DESCRIPTION));
      result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("red");

      query = factory.create(String.format("from %s where desc2 = '%s'", Color.class.getName(), BLUE_DESCRIPTION));
      result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("blue");

      query = factory.create(String.format("from %s where desc3 : 'cyan'", Color.class.getName()));
      result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("green");
   }
}
