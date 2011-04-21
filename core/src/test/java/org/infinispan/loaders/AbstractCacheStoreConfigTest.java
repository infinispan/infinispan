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
package org.infinispan.loaders;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  AbstractCacheStoreConfig }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.AbstractCacheStoreConfigTest")
public class AbstractCacheStoreConfigTest extends AbstractInfinispanTest {
   private AbstractCacheStoreConfig config;

   @BeforeMethod
   public void setUp() throws Exception {
      config = new AbstractCacheStoreConfig();
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      config = null;
   }

   @Test
   public void testIsPurgeSynchronously() {
      assert !config.isPurgeSynchronously();
   }

   @Test
   public void testSetPurgeSynchronously() {
      config.setPurgeSynchronously(true);
      assert config.isPurgeSynchronously();
   }
}