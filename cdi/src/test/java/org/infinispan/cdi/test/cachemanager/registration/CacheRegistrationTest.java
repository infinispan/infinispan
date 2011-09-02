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
package org.infinispan.cdi.test.cachemanager.registration;

import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Set;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests that configured caches are registered in the corresponding cache manager.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.registration.CacheRegistrationTest")
public class CacheRegistrationTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(CacheRegistrationTest.class.getPackage());
   }

   @Inject
   private EmbeddedCacheManager defaultCacheManager;

   @VeryLarge
   @Inject
   private EmbeddedCacheManager specificCacheManager;

   public void testCacheRegistrationInDefaultCacheManager() {
      final Set<String> cacheNames = defaultCacheManager.getCacheNames();

      assertEquals(cacheNames.size(), 2);
      assertTrue(cacheNames.contains("small"));
      assertTrue(cacheNames.contains("large"));
   }

   public void testCacheRegistrationInSpecificCacheManager() {
      final Set<String> cacheNames = specificCacheManager.getCacheNames();

      assertEquals(cacheNames.size(), 1);
      assertTrue(cacheNames.contains("very-large"));
   }
}
