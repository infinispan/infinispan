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
package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Adding another configuration to BookSearchTest so that the keys and values are stored binary in the cache.
 * The test verifies the issue ISPN-2138, i.e. no ClassCasting exception should be thrown.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.mapreduce.BookStoreAsBinarySearchTest")
public class BookStoreAsBinarySearchTest extends BookSearchTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);

      //Verification for ISPN-2138 - the following exception should not appear:
      //java.lang.ClassCastException: org.infinispan.marshall.MarshalledValue cannot be cast to org.infinispan.distexec.mapreduce.Book
      builder.storeAsBinary().enable();
      createClusteredCaches(4, "bookSearch", builder);
   }

}
