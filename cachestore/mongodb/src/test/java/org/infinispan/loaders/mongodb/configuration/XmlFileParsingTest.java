/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG_NO_SCHEMA;

/**
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@Test(groups = "unit", testName = "loaders.mongodb.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod(alwaysRun = true)
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = INFINISPAN_START_TAG_NO_SCHEMA + "\n" +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <mongodbStore xmlns=\"urn:infinispan:config:mongodb:5.3\" >\n" +
            "         <connection host=\"localhost\" port=\"27017\" timeout=\"2000\" acknowledgment=\"0\"/>\n" +
            "		  <authentication username=\"mongoUser\" password=\"mongoPass\" />\n" +
            "		  <storage database=\"infinispan_test_database\" collection=\"infispan_cachestore\" />\n" +
            "       </mongodbStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      MongoDBCacheStoreConfiguration store = (MongoDBCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert store.host().equals("localhost");
      assert store.port() == 27017;
      assert store.username().equals("mongoUser");
      assert store.password().equals("mongoPass");
      assert store.database().equals("infinispan_test_database");
      assert store.collection().equals("infispan_cachestore");
      assert store.acknowledgment() == 0;
      assert store.fetchPersistentState();
      assert store.purgeOnStartup();
      assert store.ignoreModifications();
      assert store.async().enabled();
   }

   private CacheLoaderConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().get(0);
   }
}