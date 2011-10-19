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
package org.infinispan.cdi.test.util;

import org.infinispan.cdi.interceptor.DefaultCacheKeyGenerator;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.cache.annotation.CacheDefaults;
import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CacheResult;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.cdi.util.CacheLookupHelper.getCacheKeyGenerator;
import static org.infinispan.cdi.util.CacheLookupHelper.getCacheName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.util.CacheLookupHelperTest")
public class CacheLookupHelperTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(CacheLookupHelperTest.class)
            .addClass(Foo.class)
            .addClass(Bar.class)
            .addClass(FooCacheKeyGenerator.class)
            .addClass(BarCacheKeyGenerator.class);
   }

   @Inject
   private BeanManager beanManager;

   /**
    * CacheLookupHelper#getCacheName() tests.
    */

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "method parameter must not be null")
   public void testGetCacheNameWithNullMethodParameter() {
      getCacheName(null, "", null, false);
   }

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "methodCacheName parameter must not be null")
   public void testGetCacheNameWithNullMethodCacheNameParameter() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);

      getCacheName(fooMethod, null, null, false);
   }

   public void testGetCacheNameWithMethodCacheNameWithoutDefaultAnnotation() throws Exception {
      final Method barMethod = Foo.class.getMethod("barMethod");
      final CacheResult cacheResultAnnotation = barMethod.getAnnotation(CacheResult.class);
      final String cacheName = getCacheName(barMethod, cacheResultAnnotation.cacheName(), null, false);

      assertEquals(cacheName, "bar-cache");
   }

   public void testGetCacheNameWithoutMethodCacheNameWithoutDefaultAnnotation() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final String cacheName = getCacheName(fooMethod, cacheResultAnnotation.cacheName(), null, false);

      assertEquals(cacheName, "");
   }

   public void testGetCacheNameWithoutMethodCacheNameWithoutDefaultAndGenerate() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final String cacheName = getCacheName(fooMethod, cacheResultAnnotation.cacheName(), null, true);

      assertEquals(cacheName, Foo.class.getName() + ".fooMethod(java.lang.String,java.lang.String)");
   }

   public void testGetCacheNameWithoutMethodCacheNameAndDefault() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Foo.class.getAnnotation(CacheDefaults.class);
      final String cacheName = getCacheName(fooMethod, cacheResultAnnotation.cacheName(), cacheDefaultsAnnotation, false);

      assertEquals(cacheName, "default-cache");
   }

   public void testGetCacheNameWithMethodCacheNameAndDefault() throws Exception {
      final Method barMethod = Foo.class.getMethod("barMethod");
      final CacheResult cacheResultAnnotation = barMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Foo.class.getAnnotation(CacheDefaults.class);
      final String cacheName = getCacheName(barMethod, cacheResultAnnotation.cacheName(), cacheDefaultsAnnotation, false);

      assertEquals(cacheName, "bar-cache");
   }

   public void testGetCacheNameWithoutMethodCacheNameWithoutDefault() throws Exception {
      final Method fooMethod = Bar.class.getMethod("fooMethod");
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Bar.class.getAnnotation(CacheDefaults.class);
      final String cacheName = getCacheName(fooMethod, cacheResultAnnotation.cacheName(), cacheDefaultsAnnotation, false);

      assertEquals(cacheName, "");
   }

   public void testGetCacheNameWithMethodCacheNameWithoutDefault() throws Exception {
      final Method barMethod = Bar.class.getMethod("barMethod");
      final CacheResult cacheResultAnnotation = barMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Bar.class.getAnnotation(CacheDefaults.class);
      final String cacheName = getCacheName(barMethod, cacheResultAnnotation.cacheName(), cacheDefaultsAnnotation, false);

      assertEquals(cacheName, "bar-cache");
   }

   /**
    * CacheLookupHelper#getCacheKeyGenerator() tests.
    */

   @Test(expectedExceptions = NullPointerException.class,
         expectedExceptionsMessageRegExp = "beanManager parameter must not be null")
   public void testGetCacheKeyGeneratorWithNullBeanManagerParameter() {
      getCacheKeyGenerator(null, null, null);
   }

   public void testGetCacheKeyGeneratorWithoutMethodCacheKeyGeneratorWithoutDefaultAnnotation() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), null);

      assertTrue(cacheKeyGenerator instanceof DefaultCacheKeyGenerator);
   }

   public void testGetCacheKeyGeneratorWithoutMethodCacheKeyGeneratorAndDefault() throws Exception {
      final Method fooMethod = Foo.class.getMethod("fooMethod", String.class, String.class);
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Foo.class.getAnnotation(CacheDefaults.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);

      assertTrue(cacheKeyGenerator instanceof FooCacheKeyGenerator);
   }

   public void testCacheKeyGeneratorWithMethodCacheKeyGeneratorAndDefault() throws Exception {
      final Method barMethod = Foo.class.getMethod("barMethod");
      final CacheResult cacheResultAnnotation = barMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Foo.class.getAnnotation(CacheDefaults.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);

      assertTrue(cacheKeyGenerator instanceof BarCacheKeyGenerator);
   }

   public void testCacheKeyGeneratorNameWithoutMethodCacheKeyGeneratorWithoutDefault() throws Exception {
      final Method fooMethod = Bar.class.getMethod("fooMethod");
      final CacheResult cacheResultAnnotation = fooMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Bar.class.getAnnotation(CacheDefaults.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);

      assertTrue(cacheKeyGenerator instanceof DefaultCacheKeyGenerator);
   }

   public void testGetCacheKeyGeneratorWithMethodCacheKeyGeneratorWithoutDefault() throws Exception {
      final Method barMethod = Bar.class.getMethod("barMethod");
      final CacheResult cacheResultAnnotation = barMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Bar.class.getAnnotation(CacheDefaults.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);

      assertTrue(cacheKeyGenerator instanceof BarCacheKeyGenerator);
   }

   public void testGetCacheKeyGeneratorWithANonManagedCacheKeyGenerator() throws Exception {
      final Method bazMethod = Foo.class.getMethod("bazMethod");
      final CacheResult cacheResultAnnotation = bazMethod.getAnnotation(CacheResult.class);
      final CacheDefaults cacheDefaultsAnnotation = Bar.class.getAnnotation(CacheDefaults.class);
      final CacheKeyGenerator cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);

      assertTrue(cacheKeyGenerator instanceof BazCacheKeyGenerator);
   }

   /**
    * Test classes.
    */

   @CacheDefaults(cacheName = "default-cache", cacheKeyGenerator = FooCacheKeyGenerator.class)
   static class Foo {
      @CacheResult
      public String fooMethod(String foo, String foo2) {
         return foo + foo2;
      }

      @CacheResult(cacheName = "bar-cache", cacheKeyGenerator = BarCacheKeyGenerator.class)
      public void barMethod() {
      }

      @CacheResult(cacheKeyGenerator = BazCacheKeyGenerator.class)
      public void bazMethod() {
      }
   }

   @CacheDefaults
   static class Bar {
      @CacheResult
      public void fooMethod() {
      }

      @CacheResult(cacheName = "bar-cache", cacheKeyGenerator = BarCacheKeyGenerator.class)
      public void barMethod() {
      }
   }

   static class FooCacheKeyGenerator implements CacheKeyGenerator {
      @Override
      public CacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
         return null;
      }
   }

   static class BarCacheKeyGenerator implements CacheKeyGenerator {
      @Override
      public CacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
         return null;
      }
   }

   static class BazCacheKeyGenerator implements CacheKeyGenerator {
      @Override
      public CacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
         return null;
      }
   }
}
