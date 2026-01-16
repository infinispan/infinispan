package org.infinispan.client.hotrod.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Developer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.annotation.TestForIssue;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.projection.RemoteMetaProjectionTest")
@TestForIssue(jiraKey = {"ISPN-14478", "ISPN-16489"})
public class RemoteMetaProjectionTest extends SingleHotRodServerTest {

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
      smokeTest(remoteCache);
   }

   static void smokeTest(RemoteCache<String, Developer> remoteCache) {
      Developer old1 = new Developer("james", "james@blablabla.edu", "Hibernate developer", 2004, "Hibernate developer");
      Developer dev1 = new Developer("james", "james@blablabla.edu", "Infinispan developer", 2024, "Infinispan developer");
      Developer dev2 = new Developer("jeremy", "jeremy@blablabla.edu", "Infinispan engineer", 2024, "Infinispan engineer");

      remoteCache.put("james", old1);
      remoteCache.put("james", dev1);
      remoteCache.put("jeremy", dev2);

      long version1 = remoteCache.getWithMetadata("james").getVersion();
      assertThat(version1).isPositive();
      long version2 =remoteCache.getWithMetadata("jeremy").getVersion();
      assertThat(version2).isPositive();

      Query<Object[]> query;
      List<Object[]> list;

      query = remoteCache.query("select version(d), d from io.dev.Developer d where d.biography : 'Infinispan'");
      list = query.list();

      assertThat(list).extracting(item -> item[0]).containsExactlyInAnyOrder(version1, version2);
      assertThat(list).extracting(item -> item[1]).containsExactlyInAnyOrder(dev1, dev2);

      query = remoteCache.query(
            "select d.nick, version(d), d.email, d.biography, d.contributions from io.dev.Developer d where d.biography : 'Infinispan' order by d.email");
      list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)).containsExactly(dev1.getNick(), version1, dev1.getEmail(), dev1.getBiography(), dev1.getContributions());
      assertThat(list.get(1)).containsExactly(dev2.getNick(), version2, dev2.getEmail(), dev2.getBiography(), dev2.getContributions());

      query = remoteCache.query(
            "select d, version(d) from io.dev.Developer d where d.biography : 'Infinispan' order by d.email");
      list = query.execute().list();

      assertThat(list).hasSize(2);
      assertThat(list.get(0)).containsExactly(dev1, version1);
      assertThat(list.get(1)).containsExactly(dev2, version2);

      // score

      query = remoteCache.query(
            "select d, d.email, score(d), version(d) from io.dev.Developer d order by d.email");
      list = query.execute().list();
      assertThat(list).hasSize(2);
      assertThat(list.get(0)).containsExactly(dev1, dev1.getEmail(), 1F, version1);
      assertThat(list.get(1)).containsExactly(dev2, dev2.getEmail(), 1F, version2);

      // non indexed

      query = remoteCache.query(
            "select d.nick, version(d), d.email, d.biography, d.contributions from io.dev.Developer d where d.nonIndexed = 'Infinispan developer' order by d.email");
      list = query.execute().list();
      assertThat(list).hasSize(1);
      assertThat(list.get(0)).containsExactly(dev1.getNick(), version1, dev1.getEmail(), dev1.getBiography(), dev1.getContributions());

      query = remoteCache.query(
            "select d, version(d) from io.dev.Developer d where d.nonIndexed = 'Infinispan developer' order by d.email");
      list = query.execute().list();
      assertThat(list).hasSize(1);
      assertThat(list.get(0)).containsExactly(dev1, version1);
   }
}
