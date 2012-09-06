/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.loaders;

import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.loaders.jdbm.JdbmCacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Test tree cache storing data into a cache store that requires data to be
 * serializable as per standard Java rules, such as a the JDBM cache store.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "loaders.TreeCacheWithJdbmLoaderTest")
public class TreeCacheWithJdbmLoaderTest extends TreeCacheWithLoaderTest {

   private String tmpDirectory;

   @Override
   protected void addCacheStore(LegacyStoreConfigurationBuilder cb) {
      cb.cacheStore(new JdbmCacheStore())
         .purgeSynchronously(true) // for more accurate unit testing
         .addProperty("location", tmpDirectory);
   }

   @Override
   protected void setup() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this);
      super.setup();
   }

   @Override
   protected void teardown() {
      super.teardown();
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

}
