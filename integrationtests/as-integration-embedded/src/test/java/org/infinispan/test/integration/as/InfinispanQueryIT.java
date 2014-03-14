package org.infinispan.test.integration.as;

import static junit.framework.Assert.assertEquals;

import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the Infinispan AS module integration
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class InfinispanQueryIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap.create(WebArchive.class, "query.war").addClass(InfinispanQueryIT.class).addClass(TestEntity.class).add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan:" + Version.MODULE_SLOT + " services, org.infinispan.query:" + Version.MODULE_SLOT + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = new DefaultCacheManager(builder.build());

      Cache<Long, TestEntity> cache = cm.getCache();
      cache.put(1l, new TestEntity("Adam", "Smith", 1l, "A note about Adam"));
      cache.put(2l, new TestEntity("Eve", "Smith", 2l, "A note about Eve"));
      cache.put(3l, new TestEntity("Abel", "Smith", 3l, "A note about Abel"));
      cache.put(4l, new TestEntity("Cain", "Smith", 4l, "A note about Cain"));

      SearchManager sm = Search.getSearchManager(cache);
      Query query = sm.buildQueryBuilderForClass(TestEntity.class)
            .get().keyword().onField("name").matching("Eve").createQuery();
      CacheQuery q1 = sm.getQuery(query);
      assertEquals(1, q1.getResultSize());
      assertEquals(TestEntity.class, q1.list().get(0).getClass());
      cm.stop();
   }

}
