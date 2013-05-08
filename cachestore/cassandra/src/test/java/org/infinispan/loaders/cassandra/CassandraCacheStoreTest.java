/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.cassandra;

import java.io.IOException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.CassandraCacheStoreTest")
public class CassandraCacheStoreTest extends BaseCacheStoreTest {
   private static EmbeddedServerHelper embedded;

   /**
    * Set embedded cassandra up and spawn it in a new thread.
    *
    * @throws TTransportException
    * @throws IOException
    * @throws InterruptedException
    * @throws ConfigurationException
    */
   @BeforeClass
   public static void setup() throws TTransportException, IOException, InterruptedException,
            ConfigurationException {
      embedded = new EmbeddedServerHelper();
      embedded.setup();
   }

   @AfterClass
   public static void cleanup() throws IOException {
      EmbeddedServerHelper.teardown();
      embedded = null;
   }

   @Override
   protected CacheStore createCacheStore() throws Exception {
      CassandraCacheStore cs = new CassandraCacheStore();
      CassandraCacheStoreConfig clc = new CassandraCacheStoreConfig();
      clc.setPurgeSynchronously(true);
      clc.setHost("127.0.0.1");
      clc.setAutoCreateKeyspace(true);
      clc.setKeySpace("Infinispan");
      cs.init(clc, getCache(), getMarshaller());
      cs.start();
      return cs;
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      super.testLoadAndStoreMarshalledValues();
   }

}
