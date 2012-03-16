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
package org.infinispan.profiling;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Test class that verifies how quickly Cache instances are created under different scenarios
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "profiling.CacheCreationStressTest")
public class CacheCreationStressTest extends AbstractInfinispanTest {

   public void testCreateCachesFromSameContainer() {
      long start = System.currentTimeMillis();
      CacheContainer container = new DefaultCacheManager();
      for (int i = 0; i < 1000; i++) {
         Cache cache = container.getCache(generateRandomString(20));
      }
      System.out.println("Took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
      TestingUtil.sleepThread(2000);
   }

   public static String generateRandomString(int numberOfChars) {
      Random r = new Random(System.currentTimeMillis());
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numberOfChars; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

}
