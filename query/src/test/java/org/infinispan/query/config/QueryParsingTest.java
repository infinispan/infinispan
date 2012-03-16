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

import org.infinispan.config.Configuration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "config.parsing.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {

   public void testConfigurationFileParsing() throws IOException {
      InfinispanConfiguration cfg = InfinispanConfiguration.newInfinispanConfiguration("configuration-parsing-test.xml", this.getClass().getClassLoader());
      cfg.parseGlobalConfiguration();

      final Configuration defaultConfiguration = cfg.parseDefaultConfiguration();
      defaultConfiguration.assertValid();
      assert defaultConfiguration.getIndexingProperties().size() == 0;
      assert defaultConfiguration.isIndexingEnabled() == false;

      final Map<String, Configuration> namedConfigurations = cfg.parseNamedConfigurations();

      final Configuration simpleCfg = namedConfigurations.get("simple");
      simpleCfg.assertValid();
      assert simpleCfg.isIndexingEnabled() == false;
      assert simpleCfg.getIndexingProperties().size() == 0;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable");
      memoryCfg.assertValid();
      assert memoryCfg.isIndexingEnabled();
      assert memoryCfg.getIndexingProperties().size() == 1;
      assert memoryCfg.getIndexingProperties().getProperty("hibernate.search.default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable");
      diskCfg.assertValid();
      assert diskCfg.isIndexingEnabled();
      assert diskCfg.getIndexingProperties().size() == 2;
      assert diskCfg.getIndexingProperties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.getIndexingProperties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
   }

   public void testConfigurationFileParsingWithDefaultEnabled() throws IOException {
      InfinispanConfiguration cfg = InfinispanConfiguration.newInfinispanConfiguration("configuration-parsing-test-enbledInDefault.xml", this.getClass().getClassLoader());
      cfg.parseGlobalConfiguration();

      final Configuration defaultConfiguration = cfg.parseDefaultConfiguration();
      defaultConfiguration.assertValid();
      assert defaultConfiguration.getIndexingProperties().size() == 1;
      assert defaultConfiguration.isIndexingEnabled();
      assert defaultConfiguration.getIndexingProperties().getProperty("hibernate.search.default.directory_provider").equals("someDefault");

      final Map<String, Configuration> namedConfigurations = cfg.parseNamedConfigurations();

      final Configuration nonSearchableCfg = namedConfigurations.get("not-searchable");
      nonSearchableCfg.assertValid();
      assert nonSearchableCfg.isIndexingEnabled() == false;

      final Configuration simpleCfg = namedConfigurations.get("simple");
      simpleCfg.assertValid();
      assert simpleCfg.isIndexingEnabled() == false;
      assert simpleCfg.getIndexingProperties().size() == 0;

      final Configuration memoryCfg = namedConfigurations.get("memory-searchable");
      memoryCfg.assertValid();
      assert memoryCfg.isIndexingEnabled();
      assert memoryCfg.isIndexLocalOnly() == false;
      assert memoryCfg.getIndexingProperties().size() == 1;
      assert memoryCfg.getIndexingProperties().getProperty("hibernate.search.default.directory_provider").equals("ram");

      final Configuration diskCfg = namedConfigurations.get("disk-searchable");
      diskCfg.assertValid();
      assert diskCfg.isIndexingEnabled();
      assert diskCfg.isIndexLocalOnly();
      assert diskCfg.getIndexingProperties().size() == 2;
      assert diskCfg.getIndexingProperties().getProperty("hibernate.search.default.directory_provider").equals("filesystem");
      assert diskCfg.getIndexingProperties().getProperty("hibernate.search.cats.exclusive_index_use").equals("true");
   }
}