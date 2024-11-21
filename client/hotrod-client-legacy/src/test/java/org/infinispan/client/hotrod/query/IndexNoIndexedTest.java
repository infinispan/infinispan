package org.infinispan.client.hotrod.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Reviewer;
import org.infinispan.client.hotrod.query.testdomain.protobuf.Revision;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.query.IndexNoIndexedTest")
public class IndexNoIndexedTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity("Reviewer")
            .addIndexedEntity("Revision");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("revisions", builder.build());
      return manager;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Reviewer.ReviewerSchema.INSTANCE;
   }

   @Test
   public void testIndexedEntityWithNoIndexedFields() {
      RemoteCache<Integer, Revision> remoteCache = remoteCacheManager.getCache("revisions");

      Revision revision = new Revision("ccc", "ddd");
      revision.setReviewers(Collections.singletonList(new Reviewer("aaa", "bbb")));

      assertThat(revision).isNotNull();

      assertThatThrownBy(() -> remoteCache.put(1, revision)).isInstanceOf(HotRodClientException.class)
            .hasMessageContaining("The configured indexed-entity type 'Reviewer' must be indexed");
   }
}
