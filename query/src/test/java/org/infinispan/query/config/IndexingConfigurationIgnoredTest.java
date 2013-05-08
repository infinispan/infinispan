/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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
package org.infinispan.query.config;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.assertFalse;

@Test(groups = "functional", testName = "query.config.IndexingConfigurationIgnoredTest")
public class IndexingConfigurationIgnoredTest {

   private EmbeddedCacheManager manager;

   @Test
   public void testIndexingParametersForNamedCache() {
      Cache<Object, Object> inMemory = manager.getCache("memory-searchable");
      inMemory.start();
      assertFalse(inMemory.getCacheConfiguration().indexing().properties().isEmpty(),
            "should contain definition from xml");
   }

   @BeforeMethod
   public void init() throws Exception {
      manager = TestCacheManagerFactory.fromXml("configuration-parsing-test.xml");
   }

   @AfterMethod
   public void destroy() throws Exception {
      TestingUtil.killCacheManagers(manager);
   }

}
