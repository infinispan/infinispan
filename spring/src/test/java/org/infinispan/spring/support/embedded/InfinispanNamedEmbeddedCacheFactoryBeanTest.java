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

package org.infinispan.spring.support.embedded;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanNamedEmbeddedCacheFactoryBean}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(testName = "spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBeanTest", groups = "unit")
public class InfinispanNamedEmbeddedCacheFactoryBeanTest {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final ClassPathResource NAMED_ASYNC_CACHE_CONFIG_LOCATION = new ClassPathResource(
            "named-async-cache.xml", InfinispanNamedEmbeddedCacheFactoryBeanTest.class);

   private static final EmbeddedCacheManager DEFAULT_CACHE_MANAGER = TestCacheManagerFactory.createCacheManager();

   private static final EmbeddedCacheManager PRECONFIGURED_DEFAULT_CACHE_MANAGER;

   static {
      InputStream configStream = null;
      try {
         configStream = NAMED_ASYNC_CACHE_CONFIG_LOCATION.getInputStream();
         PRECONFIGURED_DEFAULT_CACHE_MANAGER = TestCacheManagerFactory.fromStream(configStream);
      } catch (final IOException e) {
         throw new ExceptionInInitializerError(e);
      } finally {
         if (configStream != null) {
            try {
               configStream.close();
            } catch (final IOException e) {
               // Ignore
            }
         }
      }
   }

   @BeforeClass
   public static void startCacheManagers() {
      DEFAULT_CACHE_MANAGER.start();
      PRECONFIGURED_DEFAULT_CACHE_MANAGER.start();
   }

   @AfterClass
   public static void stopCacheManagers() {
      PRECONFIGURED_DEFAULT_CACHE_MANAGER.stop();
      DEFAULT_CACHE_MANAGER.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    * 
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRecognizeThatNoCacheContainerHasBeenSet()
            throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setCacheName("test.cache.Name");
      objectUnderTest.setBeanName("test.bean.Name");
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#setBeanName(java.lang.String)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldUseBeanNameAsCacheNameIfNoCacheNameHasBeenSet()
            throws Exception {
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedEmbeddedCacheFactoryBean should have used its bean name ["
               + beanName + "] as the name of the created cache. However, it didn't.", beanName,
               cache.getName());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#setCacheName(java.lang.String)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldPreferExplicitCacheNameToBeanName()
            throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedEmbeddedCacheFactoryBean should have preferred its cache name ["
               + cacheName + "] as the name of the created cache. However, it didn't.", cacheName,
               cache.getName());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#getObjectType()}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldReportTheMostDerivedObjectType()
            throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setBeanName("test.bean.Name");
      objectUnderTest.afterPropertiesSet();

      assertEquals(
               "getObjectType() should have returned the most derived class of the actual Cache "
                        + "implementation returned from getObject(). However, it didn't.",
               objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#getObject()}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldProduceANonNullInfinispanCache()
            throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();

      assertNotNull(
               "InfinispanNamedEmbeddedCacheFactoryBean should have produced a proper Infinispan cache. "
                        + "However, it produced a null Infinispan cache.", cache);
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#isSingleton()}
    * .
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();

      assertTrue(
               "InfinispanNamedEmbeddedCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
               objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#destroy()}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldStopTheCreatedInfinispanCacheWhenItIsDestroyed()
            throws Exception {
      final String cacheName = "test.cache.Name";
      final String beanName = "test.bean.Name";

      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(cacheName);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final Cache<Object, Object> cache = objectUnderTest.getObject();
      objectUnderTest.destroy();

      assertEquals(
               "InfinispanNamedEmbeddedCacheFactoryBean should have stopped the created Infinispan cache when being destroyed. "
                        + "However, the created Infinispan is not yet terminated.",
               ComponentStatus.TERMINATED, cache.getStatus());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    * 
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRejectConfigurationTemplateModeNONEIfCacheConfigurationAlreadyExistsInConfigurationFile()
            throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(PRECONFIGURED_DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setBeanName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("NONE");
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBean#afterPropertiesSet()}
    * .
    * 
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedEmbeddedCacheFactoryBeanShouldRejectConfigurationTemplateModeDEFAULTIfCacheConfigurationAlreadyExistsInConfigurationFile()
            throws Exception {
      final InfinispanNamedEmbeddedCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedEmbeddedCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanEmbeddedCacheManager(PRECONFIGURED_DEFAULT_CACHE_MANAGER);
      objectUnderTest.setCacheName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setBeanName(CACHE_NAME_FROM_CONFIGURATION_FILE);
      objectUnderTest.setConfigurationTemplateMode("DEFAULT");
      objectUnderTest.afterPropertiesSet();
   }
}
