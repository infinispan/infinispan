/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.marshall;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests defensive copy logic.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "marshall.DefensiveCopyTest")
public class DefensiveCopyTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable().defensive(true);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testOriginalReferenceSafety() {
      final Integer k = 1;
      Person person = new Person("Mr Infinispan");
      cache().put(k, person);
      assertEquals(person, cache.get(k));
      // Change referenced object
      person.setName("Ms Hibernate");
      // If defensive copies are working as expected,
      // it should be same as before
      assertEquals(new Person("Mr Infinispan"), cache.get(k));
   }

   public void testSafetyAfterRetrieving() {
      final Integer k = 2;
      Person person = new Person("Mr Coe");
      cache().put(k, person);
      Person cachedPerson = this.<Integer, Person>cache().get(k);
      assertEquals(person, cachedPerson);
      cachedPerson.setName("Mr Digweed");
      assertEquals(new Person("Mr Coe"), cache.get(k));
   }

}
