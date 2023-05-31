package org.infinispan.configuration.cache;

import static org.testng.AssertJUnit.assertFalse;
import static org.wildfly.common.Assert.assertTrue;

import java.util.Arrays;

import org.infinispan.commons.configuration.Combine;
import org.testng.annotations.Test;

/**
 * @since 15.0
 **/
@Test(testName = "configuration.cache.IndexingConfigurationBuilderTest", groups = "unit")
public class IndexingConfigurationBuilderTest {
   public void testIndexingEntitiesMerge() {
      ConfigurationBuilder one = new ConfigurationBuilder();
      one.indexing().enable().addIndexedEntities("a", "b");
      ConfigurationBuilder two = new ConfigurationBuilder();
      two.indexing().enable().addIndexedEntities("c", "d");
      two.indexing().read(one.indexing().create(), new Combine(Combine.RepeatedAttributes.MERGE, Combine.Attributes.MERGE));
      IndexingConfiguration cfg = two.indexing().create();
      assertTrue(cfg.indexedEntityTypes().containsAll(Arrays.asList("a", "b", "c", "d")));
   }

   public void testIndexingEntitiesOverride() {
      ConfigurationBuilder one = new ConfigurationBuilder();
      one.indexing().enable().addIndexedEntities("a", "b");
      ConfigurationBuilder two = new ConfigurationBuilder();
      two.indexing().enable().addIndexedEntities("c", "d");
      two.indexing().read(one.indexing().create(), new Combine(Combine.RepeatedAttributes.OVERRIDE, Combine.Attributes.MERGE));
      IndexingConfiguration cfg = two.indexing().create();
      assertTrue(cfg.indexedEntityTypes().contains("a"));
      assertTrue(cfg.indexedEntityTypes().contains("b"));
      assertFalse(cfg.indexedEntityTypes().contains("c"));
      assertFalse(cfg.indexedEntityTypes().contains("d"));
   }
}
