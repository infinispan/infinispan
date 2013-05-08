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
package org.infinispan.loaders.hbase.configuration;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.hbase.configuration.XmlFileParsingTest")
public class XmlFileParsingTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testRemoteCacheStore() throws Exception {
      String config = INFINISPAN_START_TAG +
            "   <default>\n" +
            "     <loaders>\n" +
            "       <hbaseStore xmlns=\"urn:infinispan:config:hbase:5.3\" fetchPersistentState=\"true\" autoCreateTable=\"false\" entryColumnFamily=\"ECF\" entryTable=\"ET\" " +
            "         entryValueField=\"EVF\" expirationColumnFamily=\"XCF\" expirationTable=\"XT\" expirationValueField=\"XVF\" hbaseZookeeperQuorumHost=\"myhost\" hbaseZookeeperClientPort=\"4321\" sharedTable=\"true\">\n" +
            "         <async enabled=\"true\" />\n" +
            "       </hbaseStore>\n" +
            "     </loaders>\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      HBaseCacheStoreConfiguration store = (HBaseCacheStoreConfiguration) buildCacheManagerWithCacheStore(config);
      assert !store.autoCreateTable();
      assert store.entryColumnFamily().equals("ECF");
      assert store.entryTable().equals("ET");
      assert store.entryValueField().equals("EVF");
      assert store.expirationColumnFamily().equals("XCF");
      assert store.expirationTable().equals("XT");
      assert store.expirationValueField().equals("XVF");
      assert store.hbaseZookeeperQuorumHost().equals("myhost");
      assert store.hbaseZookeeperClientPort() == 4321;
      assert store.sharedTable();
      assert store.fetchPersistentState();
      assert store.async().enabled();
   }

   private CacheLoaderConfiguration buildCacheManagerWithCacheStore(final String config) throws IOException {
      InputStream is = new ByteArrayInputStream(config.getBytes());
      cacheManager = TestCacheManagerFactory.fromStream(is);
      assert cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().size() == 1;
      return cacheManager.getDefaultCacheConfiguration().loaders().cacheLoaders().get(0);
   }
}