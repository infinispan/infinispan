/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.KeywordEntity;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.LargeTermTest")
public class LargeTermTest extends SingleHotRodServerTest {

   public static final String DESCRIPTION = "foo bar% baz";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("KeywordEntity");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("keyword", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return KeywordEntity.KeywordSchema.INSTANCE;
   }

   @Test
   public void test() {
      RemoteCache<Integer, KeywordEntity> remoteCache = remoteCacheManager.getCache("keyword");

      assertThatThrownBy(() -> remoteCache.put(1, new KeywordEntity(createLargeDescription(3000))))
            .isInstanceOf(HotRodClientException.class)
            .hasMessageContaining("bytes can be at most 32766");

      // the server continue to work
      KeywordEntity entity = new KeywordEntity(createLargeDescription(1));
      remoteCache.put(1, entity);

      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);
      assertEquals(1, queryFactory.create("from KeywordEntity where keyword : 'foo bar0 baz'").execute().hitCount().orElse(-1));
   }

   public String createLargeDescription(int times) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < times; i++) {
         String desc = DESCRIPTION.replace("%", i + "");
         builder.append(desc);
         if (i < times - 1) {
            builder.append(" ");
         }
      }
      return builder.toString();
   }
}
