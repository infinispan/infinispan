package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

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

      assertFalse(defaultConfiguration.indexing().enabled());

      Configuration simpleCfg = namedConfigurations.get("simple").build();
      assertFalse(simpleCfg.indexing().enabled());

      Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assertTrue(memoryCfg.indexing().enabled());
      assertEquals(IndexStorage.LOCAL_HEAP, memoryCfg.indexing().storage());

      Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assertTrue(diskCfg.indexing().enabled());
      assertEquals(IndexStorage.FILESYSTEM, diskCfg.indexing().storage());
      assertEquals("target/", diskCfg.indexing().path());

      Configuration replDefaults = namedConfigurations.get("repl-with-default").build();
      assertTrue(replDefaults.indexing().enabled());
   }

   public void testConfigurationFileParsingWithDefaultEnabled() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test-enabledInDefault.xml");
      Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();
      Configuration defaultConfiguration = namedConfigurations.get("default").build();

      assertTrue(defaultConfiguration.indexing().enabled());
      assertEquals(IndexStorage.LOCAL_HEAP, defaultConfiguration.indexing().storage());

      Configuration nonSearchableCfg = namedConfigurations.get("not-searchable").build();
      assertFalse(nonSearchableCfg.indexing().enabled());

      Configuration simpleCfg = namedConfigurations.get("simple").build();
      assertTrue(simpleCfg.indexing().enabled());

      Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assertTrue(memoryCfg.indexing().enabled());
      assertEquals(IndexStorage.LOCAL_HEAP, memoryCfg.indexing().storage());

      Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assertTrue(diskCfg.indexing().enabled());
      assertEquals(IndexStorage.FILESYSTEM, diskCfg.indexing().storage());
      assertEquals("target/", diskCfg.indexing().path());
   }
}
