/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.BrokenDocumentId;
import org.infinispan.query.test.BrokenProvided;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This test is to try and create a searchable cache without the proper annotations used.
 *
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class BrokenAnnotationTest extends SingleCacheManagerTest {
   Cache<?, ?> c;

   public BrokenAnnotationTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testProvided() throws Exception {
      TestQueryHelperFactory.createTestQueryHelperInstance(c, BrokenProvided.class);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testDocumentId() throws Exception {
      TestQueryHelperFactory.createTestQueryHelperInstance(c, BrokenDocumentId.class);
   }

   protected CacheManager createCacheManager() throws Exception {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      c = cm.getCache();
      return cm;
   }
}
