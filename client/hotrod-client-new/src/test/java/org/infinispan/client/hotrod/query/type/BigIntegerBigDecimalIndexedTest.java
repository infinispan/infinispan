package org.infinispan.client.hotrod.query.type;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusIndexed;
import org.infinispan.client.hotrod.query.testdomain.protobuf.CalculusIndexedSchemaImpl;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Product;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.type.BigIntegerBigDecimalIndexedTest")
public class BigIntegerBigDecimalIndexedTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("lab.indexed.CalculusIndexed");
      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return new CalculusIndexedSchemaImpl();
   }

   @Test
   public void test() {
      RemoteCache<String, CalculusIndexed> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("1", new CalculusIndexed("blablabla", BigInteger.TEN, BigDecimal.valueOf(2.2), BigDecimal.valueOf(2.2)));

      CalculusIndexed calculus = remoteCache.get("1");
      assertThat(calculus.getPurchases()).isEqualTo(10);
      assertThat(calculus.getProspect()).isEqualTo(BigDecimal.valueOf(2.2));

      Query<Product> query;
      QueryResult<Product> result;

      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.purchases > 9");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("blablabla");

      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.prospect = 2.2");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("blablabla");

      // also 2.0 match since the field prospect is annotated with @Basic and not with @Decimal
      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.prospect = 2.0");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("blablabla");

      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.prospect = 3.0");
      result = query.execute();
      assertThat(result.list()).isEmpty();

      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.decimal = 2.2");
      result = query.execute();
      assertThat(result.list()).extracting("name").containsExactly("blablabla");

      query = remoteCache.query("from lab.indexed.CalculusIndexed c where c.decimal = 2.0");
      result = query.execute();
      assertThat(result.list()).extracting("name").isEmpty();
   }
}
