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
package org.infinispan.stress;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.infinispan.Cache;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test  (groups = "stress", description = "designed to be run by hand", enabled = false)
public class MemoryCleanupTest {


   @BeforeTest
   public void createCm() {
   }

   public void testMemoryConsumption () throws InterruptedException {
      final Configuration config = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.LOCAL);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(config);

      Cache<Object,Object> cache = cm.getCache();

      long freeMemBefore = freeMemKb();
      System.out.println("freeMemBefore = " + freeMemBefore);

      for (int i =0; i < 1024 * 300; i++) {
         cache.put(i,i);
      }
      System.out.println("Free meme after: " + freeMemKb());
      System.out.println("Consumed memory: " + (freeMemBefore - freeMemKb()));
      cm.stop();
      for (int i = 0; i<10; i++) {
         System.gc();
         if (isOkay(freeMemBefore)) {
            break;
         } else {
            Thread.sleep(1000);
         }
      }
      System.out.println("Free memory at the end:" + freeMemKb());
      assert isOkay(freeMemBefore);
      
   }

   private boolean isOkay(long freeMemBefore) {
      return freeMemBefore < freeMemKb() + 0.1 * freeMemKb();
   }


   public long freeMemKb() {
      return Runtime.getRuntime().freeMemory() / 1024;
   }
}
