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

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.config.Configuration.CacheMode.LOCAL;

/**
 * @author Navin Surtani
 */

@Test(groups = "functional")
public class LocalCacheTest extends AbstractLocalQueryTest {

   protected void enhanceConfig(Configuration c) {
      // no op, meant to be overridden
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultClusteredConfig(LOCAL, true);
      c.configureIndexing().enabled(true).indexLocalOnly(false);
      enhanceConfig(c);
      return TestCacheManagerFactory.createCacheManager(c, true);
   }


   @BeforeMethod
   public void setUp() throws Exception {
      cache = cacheManager.getCache();

      qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache, Person.class, AnotherGrassEater.class);

      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");

      person5 = new Person();
      person5.setName("Smelly Cat");
      person5.setBlurb("Eats fish");

      anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");

      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);
      cache.put(anotherGrassEaterKey, anotherGrassEater);
   }
}
