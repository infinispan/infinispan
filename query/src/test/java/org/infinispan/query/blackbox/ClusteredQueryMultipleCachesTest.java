/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.query.blackbox;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Tests for testing clustered queries functionality on multiple cache instances
 * (In these tests we have two caches in each CacheManager)
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryMultipleCachesTest")
public class ClusteredQueryMultipleCachesTest extends ClusteredQueryTest {

   Cache<String, Person> cacheBMachine1, cacheBMachine2;
   Person person5;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg.indexing().enable().indexLocalOnly(true).addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      String[] cacheNames = { "cacheA", "cacheB" };
      List<List<Cache<String, Person>>> caches = createClusteredCaches(2, cacheCfg, cacheNames);
      cacheAMachine1 = caches.get(0).get(0);
      cacheAMachine2 = caches.get(1).get(0);
      cacheBMachine1 = caches.get(0).get(1);
      cacheBMachine2 = caches.get(1).get(1);
   }

   @Override
   protected void prepareTestData() {
      super.prepareTestData();

      person5 = new Person();
      person5.setName("People In Another Cache");
      person5.setBlurb("Also eats grass");
      person5.setAge(5);

      cacheBMachine2.put("anotherNewOne", person5);
   }

}
