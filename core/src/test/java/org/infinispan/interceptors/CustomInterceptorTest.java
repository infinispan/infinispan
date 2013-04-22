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
package org.infinispan.interceptors;

import static org.testng.AssertJUnit.*;
import static org.infinispan.test.TestingUtil.withCacheManager;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "interceptors.CustomInterceptorTest")
public class CustomInterceptorTest extends AbstractInfinispanTest {

   public void testCustomInterceptorProperties() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(new FooInterceptor()).position(Position.FIRST).addProperty("foo", "bar");
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            final Cache<Object,Object> cache = cm.getCache();
            CommandInterceptor i = cache.getAdvancedCache().getInterceptorChain().get(0);
            assertTrue("Expecting FooInterceptor in the interceptor chain", i instanceof FooInterceptor);
            assertEquals("bar", ((FooInterceptor)i).getFoo());
         }
      });
   }

   @Test(expectedExceptions=ConfigurationException.class)
   public void testMissingPosition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(new FooInterceptor());
      TestCacheManagerFactory.createCacheManager(builder);
   }

}
