package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Color;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.MultipleIndexFieldAnnotationsTest")
public class MultipleIndexFieldAnnotationsTest extends SingleHotRodServerTest {

   // descriptions taken are from Wikipedia https://en.wikipedia.org/wiki/*
   public static final String RED_DESCRIPTION = "Red is the color at the long wavelength end of the visible spectrum of light";
   public static final String GREEN_DESCRIPTION = "Green is the color between cyan and yellow on the visible spectrum.";
   public static final String BLUE_DESCRIPTION = "Blue is one of the three primary colours in the RYB colour model (traditional color theory), as well as in the RGB (additive) colour model.";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity("Color");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("colors", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Color.ColorSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Integer, Color> remoteCache = remoteCacheManager.getCache("colors");

      Color color1 = new Color();
      color1.setName("red");
      color1.setDescription(RED_DESCRIPTION);
      remoteCache.put(1, color1);

      Color color2 = new Color();
      color2.setName("green");
      color2.setDescription(GREEN_DESCRIPTION);
      remoteCache.put(2, color2);

      Color color3 = new Color();
      color3.setName("blue");
      color3.setDescription(BLUE_DESCRIPTION);
      remoteCache.put(3, color3);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
      Query<Color> query = queryFactory.create("from Color where name = 'red'");
      QueryResult<Color> result = query.execute();

      assertThat(result.hitCount()).hasValue(1L);
      assertThat(result.list()).extracting("name").contains("red");
   }
}
