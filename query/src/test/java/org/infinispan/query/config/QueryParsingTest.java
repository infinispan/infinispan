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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

@Test(groups = "unit", testName = "config.parsing.QueryParsingTest")
public class QueryParsingTest extends AbstractInfinispanTest {

   public void testQueryConfig() throws Exception {
      String config = TestingUtil.INFINISPAN_START_TAG + "\n" +
            "   <global>\n" +
            "      <transport clusterName=\"demoCluster\" />\n" +
            "   </global>\n" +
            "   <default>\n" +
            "      <clustering mode=\"replication\" />\n" +
            "      <indexing enabled=\"true\" indexLocalOnly=\"true\" />\n" +
            "   </default>\n" +
            TestingUtil.INFINISPAN_END_TAG;

      System.out.println("Using test configuration:\n\n" + config + "\n");

      InputStream is = new ByteArrayInputStream(config.getBytes());
      InputStream schema = InfinispanConfiguration.findSchemaInputStream();
      assert schema != null;
      InfinispanConfiguration c = InfinispanConfiguration.newInfinispanConfiguration(is, schema);
      GlobalConfiguration gc = c.parseGlobalConfiguration();
      assert gc.getTransportClass().equals(JGroupsTransport.class.getName());
      assert gc.getClusterName().equals("demoCluster");

      Configuration def = c.parseDefaultConfiguration();
      assert def.isIndexingEnabled();
      assert def.isIndexLocalOnly();

      // test cloneability
      Configuration dolly = def.clone();
      assert dolly.isIndexingEnabled();
      assert dolly.isIndexLocalOnly();

      // test mergeability
      Configuration other = new Configuration().fluent()
         .storeAsBinary()
         .locking().useLockStriping(false)
         .build();
      other.applyOverrides(dolly);
      assert other.isStoreAsBinary();
      assert !other.isUseLockStriping();
      assert other.isIndexingEnabled();
      assert other.isIndexLocalOnly();

      CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager(def);
      try {
         Cache<Object, Object> cache = cm.getCache("test");
         cache.stop();
      }
      finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}