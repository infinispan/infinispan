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

package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.infinispan.test.TestingUtil.withCacheManager;

@Test(groups = {"performance", "manual"})
public class CreateThousandCachesTest {
   public void doTest() {
      System.out.println("Starting... ");
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            List<Cache<?, ?>> thousandCaches = new LinkedList<Cache<?, ?>>();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
               thousandCaches.add(cm.getCache("cache" + i));
            }
            System.out.println("Created 1000 basic caches in " + (System.currentTimeMillis() - start) + " millis");
         }
      });
   }
}
