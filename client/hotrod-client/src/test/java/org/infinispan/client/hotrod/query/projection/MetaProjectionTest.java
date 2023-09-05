package org.infinispan.client.hotrod.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Developer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.projection.MetaProjectionTest")
@TestForIssue(jiraKey = "ISPN-14478")
public class MetaProjectionTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("io.dev.Developer");
      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Developer.DeveloperSchema.INSTANCE;
   }

   @Test
   public void testVersionProjection() {
      RemoteCache<String, Developer> remoteCache = remoteCacheManager.getCache();

      remoteCache.put("open-contributor", new Developer("iamopen", "iamopen@redmail.io", "Hibernate developer", 2000));
      remoteCache.put("open-contributor", new Developer("iamopen", "iamopen@redmail.io", "Infinispan developer", 2000));
      remoteCache.put("another-contributor", new Developer("mycodeisopen", "mycodeisopen@redmail.io",
            "Infinispan engineer", 799));

      MetadataValue<Developer> metadata = remoteCache.getWithMetadata("open-contributor");
      assertThat(metadata.getVersion()).isEqualTo(2L);
      metadata = remoteCache.getWithMetadata("another-contributor");
      assertThat(metadata.getVersion()).isEqualTo(3L); // version is global to the cache - not to the entry

      Query<Object[]> query = remoteCache.query(
            "select d.nick, version(d), d.email, d.biography, d.contributions from io.dev.Developer d where d.biography : 'Infinispan' order by d.email");
      List<Object[]> list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)).containsExactly("iamopen", 2L, "iamopen@redmail.io",
            "Infinispan developer", 2000);
      assertThat(list.get(1)).containsExactly("mycodeisopen", 3L, "mycodeisopen@redmail.io",
            "Infinispan engineer", 799);

      query = remoteCache.query(
            "select d, version(d) from io.dev.Developer d where d.biography : 'Infinispan' order by d.email");
      list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)[0]).isNotNull().isInstanceOf(Developer.class);
      assertThat(list.get(0)[1]).isEqualTo(2L);
      assertThat(list.get(1)[0]).isNotNull().isInstanceOf(Developer.class);
      assertThat(list.get(1)[1]).isEqualTo(3L);
   }

}
