/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.config;

import java.io.IOException;
import java.util.Map;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "config.parsing.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {

   public void testConfigurationFileParsing() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test.xml");
      Configuration defaultConfiguration = holder.getDefaultConfigurationBuilder().build();

      assert defaultConfiguration.indexing().properties().size() == 0;
      assert defaultConfiguration.indexing().enabled() == false;

      final Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();

      final Configuration simpleCfg = namedConfigurations.get("simple").build();
      assert simpleCfg.indexing().enabled() == false;
      assert simpleCfg.indexing().properties().size() == 0;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assert memoryCfg.indexing().enabled();
      assert memoryCfg.indexing().properties().size() == 2;
      assert memoryCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assert diskCfg.indexing().enabled();
      assert diskCfg.indexing().properties().size() == 3;
      assert diskCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.indexing().properties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
   }

   public void testConfigurationFileParsingWithDefaultEnabled() throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parseFile("configuration-parsing-test-enbledInDefault.xml");
      Configuration defaultConfiguration = holder.getDefaultConfigurationBuilder().build();

      assert defaultConfiguration.indexing().properties().size() == 2;
      assert defaultConfiguration.indexing().enabled();
      assert defaultConfiguration.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("someDefault");

      final Map<String, ConfigurationBuilder> namedConfigurations = holder.getNamedConfigurationBuilders();

      final Configuration nonSearchableCfg = namedConfigurations.get("not-searchable").build();
      assert nonSearchableCfg.indexing().enabled() == false;

      final Configuration simpleCfg = namedConfigurations.get("simple").build();
      assert simpleCfg.indexing().enabled() == true;
      assert simpleCfg.indexing().properties().size() == 2;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable").build();
      assert memoryCfg.indexing().enabled();
      assert memoryCfg.indexing().indexLocalOnly() == false;
      assert memoryCfg.indexing().properties().size() == 2;
      assert memoryCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable").build();
      assert diskCfg.indexing().enabled();
      assert diskCfg.indexing().indexLocalOnly();
      assert diskCfg.indexing().properties().size() == 3;
      assert diskCfg.indexing().properties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.indexing().properties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
   }
}