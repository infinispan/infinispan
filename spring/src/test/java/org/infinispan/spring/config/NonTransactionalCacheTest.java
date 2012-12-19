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

import org.springframework.cache.annotation.Cacheable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Non transaction cacheable test.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "spring.config.NonTransactionalCacheTest")
@ContextConfiguration
public class NonTransactionalCacheTest extends AbstractTestNGSpringContextTests {

   public interface ICachedMock {
      Integer get();
   }

   public static class CachedMock implements ICachedMock {
      private Integer value = 0;

      @Override
      @Cacheable(value = "cachedMock")
      public Integer get() {
         return ++this.value;
      }
   }

   @Resource(name = "mock")
   private ICachedMock mock;

   @Test
   public void testCalls() {
      assertEquals(Integer.valueOf(1), this.mock.get());
      assertEquals(Integer.valueOf(1), this.mock.get());
   }


}
