/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.configuration.module;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.INFINISPAN_END_TAG;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * ExtendedParserTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "unit", testName = "configuration.module.ExtendedParserTest")
public class ExtendedParserTest {

   public void testExtendedParser() throws IOException {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "      <sample-element xmlns=\"urn:infinispan:config:mymodule:5.2\" sample-attribute=\"test-value\" />\n" +
            "   </default>\n" +
            INFINISPAN_END_TAG;
      assertCacheConfiguration(config);
   }

   private void assertCacheConfiguration(String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = parserRegistry.parse(is);
      DefaultCacheManager cacheManager = new DefaultCacheManager(holder, false);
      Assert.assertEquals(cacheManager.getDefaultCacheConfiguration().module(MyModuleConfiguration.class).attribute(), "test-value");
   }
}
