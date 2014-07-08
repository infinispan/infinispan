package org.infinispan.query.config;

import java.io.IOException;
import java.util.Map;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.config.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {

   public void testConfigurationFileParsing() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test.xml");
      Configuration defaultConfiguration = holder.getDefaultConfigurationBuilder().build();

      assert defaultConfiguration.indexing().properties().size() == 0;
      assert !defaultConfiguration.indexing().index().isEnabled();

      final Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();

      final Configuration simpleCfg = namedConfigurations.get("simple").build();
      assert !simpleCfg.indexing().index().isEnabled();
      assert simpleCfg.indexing().properties().size() == 0;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assert memoryCfg.indexing().index().isEnabled();
      assert memoryCfg.indexing().properties().size() == 2;
      assert memoryCfg.indexing().properties().getProperty("default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assert diskCfg.indexing().index().isEnabled();
      assert diskCfg.indexing().properties().size() == 3;
      assert diskCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.indexing().properties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
      
      final Configuration replDefaults = namedConfigurations.get("repl-with-default").build();
      assert replDefaults.indexing().index().isEnabled();
      assert !replDefaults.indexing().properties().isEmpty();
   }

   public void testConfigurationFileParsingWithDefaultEnabled() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test-enbledInDefault.xml");
      Configuration defaultConfiguration = holder.getDefaultConfigurationBuilder().build();

      assert defaultConfiguration.indexing().properties().size() == 2;
      assert defaultConfiguration.indexing().index().isEnabled();
      assert defaultConfiguration.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("someDefault");

      final Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();

      final Configuration nonSearchableCfg = namedConfigurations.get("not-searchable").build();
      assert !nonSearchableCfg.indexing().index().isEnabled();

      final Configuration simpleCfg = namedConfigurations.get("simple").build();
      assert simpleCfg.indexing().index().isEnabled();
      assert simpleCfg.indexing().properties().size() == 2;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assert memoryCfg.indexing().index().isEnabled();
      assert !memoryCfg.indexing().index().isLocalOnly();
      assert memoryCfg.indexing().properties().size() == 2;
      assert memoryCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assert diskCfg.indexing().index().isEnabled();
      assert diskCfg.indexing().index().isLocalOnly();
      assert diskCfg.indexing().properties().size() == 3;
      assert diskCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.indexing().properties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
   }
}