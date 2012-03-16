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

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;

import org.infinispan.Cache;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanDefaultCacheFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/InfinispanDefaultCacheFactoryBeanContextTest.xml")
@Test(testName = "spring.support.InfinispanDefaultCacheFactoryBeanContextTest", groups = "unit")
public class InfinispanDefaultCacheFactoryBeanContextTest extends AbstractTestNGSpringContextTests {

   private static final String DEFAULT_CACHE_NAME = "testDefaultCache";

   @Test
   public final void shouldProduceANonNullCache() {
      final Cache<Object, Object> testDefaultCache = this.applicationContext.getBean(
               DEFAULT_CACHE_NAME, Cache.class);

      assertNotNull(
               "Spring application context should contain an Infinispan cache under the bean name \""
                        + DEFAULT_CACHE_NAME + "\". However, it doesn't.", testDefaultCache);
   }

   @Test
   public final void shouldAlwaysReturnTheSameCache() {
      final Cache<Object, Object> testDefaultCache1 = this.applicationContext.getBean(
               DEFAULT_CACHE_NAME, Cache.class);
      final Cache<Object, Object> testDefaultCache2 = this.applicationContext.getBean(
               DEFAULT_CACHE_NAME, Cache.class);

      assertSame(
               "InfinispanDefaultCacheFactoryBean should always return the same cache instance when being "
                        + "called repeatedly. However, the cache instances are not the same.",
               testDefaultCache1, testDefaultCache2);
   }
}
