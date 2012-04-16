/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.distribution.virtualnodes;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;

import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(testName = "distribution.virtualnodes.VNodesXmlTest", groups = "functional")
public class VNodesXmlTest extends AbstractInfinispanTest {
   public void testParseFile() throws Exception {
      String config = INFINISPAN_START_TAG +
            "<global><transport /></global>" +
            "   <default>\n" +
            "      <clustering mode=\"d\">\n" +
            "          <hash numVirtualNodes=\"5000\" />" +
            "      </clustering>\n" +
            "   </default>\n" +
            "   <namedCache name=\"x\" />" +
            TestingUtil.INFINISPAN_END_TAG;


      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parse(is);
      org.infinispan.configuration.cache.Configuration defaultCfg = holder.getDefaultConfigurationBuilder().build();
      org.infinispan.configuration.cache.Configuration namedCfg = holder.getNamedConfigurationBuilders().get("x").build();

      for (org.infinispan.configuration.cache.Configuration c : Arrays.asList(defaultCfg, namedCfg)) {
         Assert.assertEquals(c.clustering().cacheMode(), CacheMode.DIST_SYNC);
         Assert.assertEquals(c.clustering().hash().numVirtualNodes(), 5000);
         // Legacy adapter
         Configuration legacy = LegacyConfigurationAdaptor.adapt(c);
         Assert.assertEquals(legacy.getCacheMode().toString(), CacheMode.DIST_SYNC.toString());
         Assert.assertEquals(legacy.getNumVirtualNodes(), 5000);
      }
   }
}
