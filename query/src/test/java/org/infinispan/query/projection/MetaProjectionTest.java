package org.infinispan.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.query.model.Developer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.projection.MetaProjectionTest")
@TestForIssue(jiraKey = "ISPN-14478")
public class MetaProjectionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Developer.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test
   public void testVersionProjection() {
      Cache<Object, Developer> cache = cacheManager.getCache();

      cache.getAdvancedCache()
            .put("open-contributor", new Developer("iamopen", "iamopen@redmail.io", "Infinispan developer", 2000),
            new EmbeddedMetadata.Builder().version(new NumericVersion(1)).build());

      cache.put("another-contributor", new Developer("mycodeisopen", "mycodeisopen@redmail.io", "Infinispan engineer",
            799));

      String ickle = String.format(
            "select d.nick, version(d), d.email, d.biography, d.contributions from %s d where d.biography : 'Infinispan' order by d.email",
            Developer.class.getName());
      Query<Object[]> query = cache.query(ickle);
      List<Object[]> list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)).containsExactly("iamopen", new NumericVersion(1), "iamopen@redmail.io",
            "Infinispan developer", 2000);
      assertThat(list.get(1)).containsExactly("mycodeisopen", null, "mycodeisopen@redmail.io",
            "Infinispan engineer", 799);

      ickle = String.format(
            "select d.nick, version(d), d.email, d.biography, d.contributions from %s d where d.biography : 'developer'",
            Developer.class.getName());
      query = cache.query(ickle);
      list = query.execute().list();

      assertThat(list).hasSize(1);
      assertThat(list.get(0)).containsExactly("iamopen", new NumericVersion(1), "iamopen@redmail.io",
            "Infinispan developer", 2000);

      ickle = String.format(
            "select version(d) from %s d where d.biography : 'developer'",
            Developer.class.getName());
      query = cache.query(ickle);
      list = query.execute().list();

      assertThat(list).hasSize(1);
      assertThat(list.get(0)).containsExactly(new NumericVersion(1));

      ickle = String.format(
            "select d, version(d) from %s d where d.biography : 'Infinispan' order by d.email",
            Developer.class.getName());
      query = cache.query(ickle);
      list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)[0]).isNotNull().isInstanceOf(Developer.class);
      assertThat(list.get(0)[1]).isEqualTo(new NumericVersion(1));
   }
}
