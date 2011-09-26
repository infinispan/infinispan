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
package org.infinispan.cdi.test.cachemanager.external;

import org.infinispan.AdvancedCache;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;


/**
 * Tests for a cache container defined by some external mechanism.
 *
 * @author Pete Muir
 * @see Config
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.external.ExternalCacheContainerTest")
public class ExternalCacheContainerTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addPackage(ExternalCacheContainerTest.class.getPackage());
   }

   @Inject
   @Large
   private AdvancedCache<?, ?> largeCache;

   @Inject
   @Quick
   private AdvancedCache<?, ?> quickCache;

   public void testLargeCache() {
      assertEquals(largeCache.getConfiguration().getEvictionMaxEntries(), 100);
   }

   public void testQuickCache() {
      assertEquals(quickCache.getConfiguration().getExpirationWakeUpInterval(), 1);
   }
}
