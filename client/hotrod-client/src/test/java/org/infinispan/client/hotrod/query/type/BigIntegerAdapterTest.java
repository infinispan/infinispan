package org.infinispan.client.hotrod.query.type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.Product;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.type.BigIntegerAdapterTest")
public class BigIntegerAdapterTest extends SingleHotRodServerTest {

   public static final long OVER_INTEGER_VALUE = (long) Integer.MAX_VALUE + 1000;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("store.product.Product");
      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Product.ProductSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<String, Product> remoteCache = remoteCacheManager.getCache();

      remoteCache.putAll(Product.data());

      Product product = remoteCache.get("1");
      assertThat(product.getPurchases()).isEqualTo(OVER_INTEGER_VALUE);
      assertThat(product.getMoment().getEpochSecond()).isEqualTo(1675769531);
      assertThat(product.getMoment().getNano()).isEqualTo(123000000); // this is the max precision we have at the moment

      Query<Product> query = remoteCache.query("from store.product.Product p where p.name = 'pilsner urquell'");
      QueryResult<Product> result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("Pilsner Urquell");

      query = remoteCache.query("from store.product.Product p where p.code = 178128739123");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("Lavazza Coffee");

      query = remoteCache.query("from store.product.Product p where p.price < 30 order by p.price desc");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("Pilsner Urquell", "Lavazza Coffee");

      query = remoteCache.query("from store.product.Product p where p.description : 'gym'");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("Puma Backpack");
   }
}
