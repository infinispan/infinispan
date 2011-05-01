/**
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *   ~
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

package org.infinispan.spring.support;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanDefaultCacheFactoryBean}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(testName = "spring.support.InfinispanDefaultCacheFactoryBeanTest", groups = "unit")
public class InfinispanDefaultCacheFactoryBeanTest {

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#afterPropertiesSet()}.
    * 
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void afterPropertiesSetShouldThrowAnIllegalStateExceptionIfNoCacheContainerHasBeenSet()
            throws Exception {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#getObject()}.
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldProduceANonNullInfinispanCache()
            throws Exception {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanCacheContainer(new DefaultCacheManager());
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertNotNull(
               "InfinispanDefaultCacheFactoryBean should have produced a proper Infinispan cache. "
                        + "However, it produced a null Infinispan cache.", cache);
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#getObjectType()}.
    * 
    * @throws Exception
    */
   @Test
   public final void getObjectTypeShouldReturnTheMostDerivedTypeOfTheProducedInfinispanCache()
            throws Exception {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanCacheContainer(new DefaultCacheManager());
      objectUnderTest.afterPropertiesSet();

      assertEquals(
               "getObjectType() should have returned the produced Infinispan cache's most derived type. "
                        + "However, it returned a more generic type.", objectUnderTest.getObject()
                        .getClass(), objectUnderTest.getObjectType());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#isSingleton()}.
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();

      assertTrue(
               "InfinispanDefaultCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
               objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.InfinispanDefaultCacheFactoryBean#destroy()}.
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanDefaultCacheFactoryBeanShouldStopTheCreatedInfinispanCacheWhenItIsDestroyed()
            throws Exception {
      final InfinispanDefaultCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanDefaultCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanCacheContainer(new DefaultCacheManager());
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();
      objectUnderTest.destroy();

      assertEquals(
               "InfinispanDefaultCacheFactoryBean should have stopped the created Infinispan cache when being destroyed. "
                        + "However, the created Infinispan is not yet terminated.",
               ComponentStatus.TERMINATED, cache.getStatus());
   }
}
