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

package org.infinispan.loaders.mongodb;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.mongodb.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@Test(testName = "loaders.remote.MongoDBCacheStoreTest", groups = "mongodb")
public class MongoDBCacheStoreTest extends BaseCacheStoreTest {
   private static final Log log = LogFactory.getLog(MongoDBCacheStoreTest.class, Log.class);

   private MongoDBCacheStore cacheStore;

   @Override
   protected CacheStore createCacheStore() throws Exception {
      String hostname = System.getProperty("MONGODB_HOSTNAME");
      if (hostname == null || "".equals(hostname)) {
         hostname = "127.0.0.1";
      }

      int port = 27017;
      String configurationPort = System.getProperty("MONGODB_PORT");
      try {
         if (configurationPort != null && !"".equals(configurationPort)) {
            port = Integer.parseInt(configurationPort);
         }
      } catch (NumberFormatException e) {
         throw log.mongoPortIllegalValue(configurationPort);
      }
      log.runningTest(hostname, port);

      MongoDBCacheStoreConfig config = new MongoDBCacheStoreConfig(hostname, port, 2000, "", "", "infinispan_test_database", "infinispan_indexes", -1);
      config.setPurgeSynchronously(true);

      cacheStore = new MongoDBCacheStore();
      cacheStore.init(config, getCache(), getMarshaller());
      cacheStore.start();
      return cacheStore;
   }

   @Override
   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      cacheStore.clear();
   }
}
