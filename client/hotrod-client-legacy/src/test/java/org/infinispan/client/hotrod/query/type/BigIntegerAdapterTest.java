package org.infinispan.client.hotrod.query.type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.math.BigInteger;
import java.time.Instant;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Product;
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
      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Product.ProductSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<String, Product> remoteCache = remoteCacheManager.getCache();

      Product p1 = new Product("Pilsner Urquell", 2178129121111L, 23.78,
            "Pilsner Urquell is a lager beer brewed by the Pilsner Urquell Brewery in Plzeň, Czech Republic. Pilsner Urquell was the world's first pale lager, and its popularity meant it was much copied, and named pils, pilsner or pilsener. It is hopped with Saaz hops, a noble hop variety which is a key element in its flavour profile, as is the use of soft water.",
            BigInteger.valueOf(OVER_INTEGER_VALUE), Instant.ofEpochSecond(1675769531, 123000000));
      Product p2 = new Product("Lavazza Coffee", 178128739123L, 10.99,
            "Lavazza imports coffee from around the world, including Brazil, Colombia, Guatemala, Costa Rica, Honduras, Uganda, Indonesia, the United States and Mexico.\n Branded as \"Italy's Favourite Coffee,\" the company claims that 16 million out of the 20 million coffee purchasing families in Italy choose Lavazza.",
            BigInteger.valueOf(OVER_INTEGER_VALUE), Instant.ofEpochSecond(1675769531, 123000000));
      Product p3 = new Product("Puma Backpack", 21233131131L, 40.99,
            "Lightweight and practical gym bag made of durable material, which can be carried as a backpack. This classic gym sack slings easily over the shoulder and for carrying smaller loads.",
            BigInteger.valueOf(OVER_INTEGER_VALUE), Instant.ofEpochSecond(1675769531, 123000000));

      remoteCache.put("1", p1);
      remoteCache.put("2", p2);
      remoteCache.put("3", p3);

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
