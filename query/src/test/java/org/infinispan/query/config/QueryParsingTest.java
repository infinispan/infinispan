package org.infinispan.query.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.config.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {

   public void testConfigurationFileParsing() throws IOException {
      ConfigurationBuilderHolder holder = TestCacheManagerFactory.parseFile("configuration-parsing-test.xml", false);
      Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();
      Configuration defaultConfiguration = namedConfigurations.get("default").build();

      assertEquals(defaultConfiguration.indexing().properties().size(), 0);
      assertFalse(defaultConfiguration.indexing().enabled());

      Configuration simpleCfg = namedConfigurations.get("simple").build();
      assertFalse(simpleCfg.indexing().enabled());
      assertEquals(simpleCfg.indexing().properties().size(), 0);

      Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assertTrue(memoryCfg.indexing().enabled());
      assertTrue(memoryCfg.indexing().properties().isEmpty());
      assertEquals(IndexStorage.LOCAL_HEAP, memoryCfg.indexing().storage());

      Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assertTrue(diskCfg.indexing().enabled());
      assertTrue(diskCfg.indexing().properties().isEmpty());
      assertEquals(diskCfg.indexing().storage(), IndexStorage.FILESYSTEM);
      assertEquals(diskCfg.indexing().path(), "target/");

      Configuration replDefaults = namedConfigurations.get("repl-with-default").build();
      assertTrue(replDefaults.indexing().enabled());
      assertFalse(replDefaults.indexing().properties().isEmpty());
   }

   public void testConfigurationFileParsingWithDefaultEnabled() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test-enbledInDefault.xml");
      Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();
      Configuration defaultConfiguration = namedConfigurations.get("default").build();

      assertTrue(defaultConfiguration.indexing().properties().isEmpty());
      assertTrue(defaultConfiguration.indexing().enabled());
      assertEquals(IndexStorage.LOCAL_HEAP, defaultConfiguration.indexing().storage());

      Configuration nonSearchableCfg = namedConfigurations.get("not-searchable").build();
      assertFalse(nonSearchableCfg.indexing().enabled());

      Configuration simpleCfg = namedConfigurations.get("simple").build();
      assertTrue(simpleCfg.indexing().enabled());
      assertTrue(simpleCfg.indexing().properties().isEmpty());

      Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assertTrue(memoryCfg.indexing().enabled());
      assertTrue(memoryCfg.indexing().properties().isEmpty());
      assertEquals(memoryCfg.indexing().storage(), IndexStorage.LOCAL_HEAP);

      Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assertTrue(diskCfg.indexing().enabled());
      assertTrue(diskCfg.indexing().properties().isEmpty());
      assertEquals(diskCfg.indexing().storage(), IndexStorage.FILESYSTEM);
      assertEquals(diskCfg.indexing().path(), "target/");
   }
}
