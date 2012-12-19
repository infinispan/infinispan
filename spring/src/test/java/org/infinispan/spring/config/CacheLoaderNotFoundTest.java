/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

package org.infinispan.spring.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "spring.config.CacheLoaderNotFoundTest")
@ContextConfiguration
public class CacheLoaderNotFoundTest extends AbstractTestNGSpringContextTests {

   @Autowired @Qualifier("cacheManager")
   private CacheManager cm;

   @BeforeClass
   @Override
   protected void springTestContextPrepareTestInstance() throws Exception {
      try {
         super.springTestContextPrepareTestInstance();
         fail("Show have thrown an error indicating issues with the cache loader");
      } catch (IllegalStateException e) {
      }
   }

   @Test
   public void testCacheManagerExists() {
      Assert.assertNull(cm);
   }

}
