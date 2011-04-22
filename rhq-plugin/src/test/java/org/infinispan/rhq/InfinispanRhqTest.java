/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.rhq;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Standalone cache for infinispan testing
 *
 * @author Heiko W. Rupp
 */
public class InfinispanRhqTest {

   private static final String MY_CUSTOM_CACHE = "myCustomCache";

   public static void main(String[] args) throws InterruptedException {

      GlobalConfiguration myGlobalConfig = new GlobalConfiguration();
      // org.infinispan:cache-name=[global],jmx-resource=CacheManager
      myGlobalConfig.setJmxDomain("org.infinispan");
      myGlobalConfig.setExposeGlobalJmxStatistics(true);
      EmbeddedCacheManager manager = new DefaultCacheManager(myGlobalConfig);

      // org.infinispan:cache-name=myCustomcache(local),jmx-resource=CacheMgmgtInterceptor
      // org.infinispan:cache-name=myCustomcache(local),jmx-resource=MvccLockManager
      // org.infinispan:cache-name=myCustomcache(local),jmx-resource=TxInterceptor

      Configuration config = new Configuration();
      config.setExposeJmxStatistics(true);
      config.setEvictionMaxEntries(123);
      config.setExpirationMaxIdle(180000);
      
      manager.defineConfiguration(MY_CUSTOM_CACHE, config);
      Cache<String,String> cache = manager.getCache(MY_CUSTOM_CACHE);

      cache.put("myKey", "myValue");

      int i = 0;
      while (i < Integer.MAX_VALUE) {
         Thread.sleep(12000);
         cache.put("key" + i, String.valueOf(i));
         cache.get("key" + ((int)(10000 * Math.random())));
         i++;
         if (i%10 == 0) {
            System.out.print(".");
            System.out.flush();
         }
      }
   }
}
