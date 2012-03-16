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

package org.infinispan.spring.support.remote;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanNamedRemoteCacheFactoryBean}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(testName = "spring.support.remote.InfinispanNamedRemoteCacheFactoryBeanTest", groups = "functional")
public class InfinispanNamedRemoteCacheFactoryBeanTest extends SingleCacheManagerTest {

   private static final String TEST_BEAN_NAME = "test.bean.Name";

   private static final String TEST_CACHE_NAME = "test.cache.Name";

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      cache = cacheManager.getCache(TEST_CACHE_NAME);
      cache = cacheManager.getCache(TEST_BEAN_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 19733);
      remoteCacheManager = new RemoteCacheManager("localhost", hotrodServer.getPort());
   }

   @AfterClass
   public void destroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#afterPropertiesSet()}
    * .
    * 
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void infinispanNamedRemoteCacheFactoryBeanShouldRecognizeThatNoCacheContainerHasBeenSet()
            throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#setBeanName(java.lang.String)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldUseBeanNameAsCacheNameIfNoCacheNameHasBeenSet()
            throws Exception {
      final String beanName = TEST_BEAN_NAME;

      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setBeanName(beanName);
      objectUnderTest.afterPropertiesSet();

      final BasicCache<String, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedRemoteCacheFactoryBean should have used its bean name ["
               + beanName + "] as the name of the created cache. However, it didn't.", beanName,
               cache.getName());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#setCacheName(java.lang.String)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldPreferExplicitCacheNameToBeanName()
            throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      final BasicCache<String, Object> cache = objectUnderTest.getObject();

      assertEquals("InfinispanNamedRemoteCacheFactoryBean should have preferred its cache name ["
               + TEST_CACHE_NAME + "] as the name of the created cache. However, it didn't.",
               TEST_CACHE_NAME, cache.getName());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#getObjectType()}.
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldReportTheMostDerivedObjectType()
            throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<Object, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<Object, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      assertEquals(
               "getObjectType() should have returned the most derived class of the actual Cache "
                        + "implementation returned from getObject(). However, it didn't.",
               objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
   }
   
   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#getObject()}.
    * 
    * @throws Exception
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldProduceANonNullInfinispanCache()
            throws Exception {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();
      objectUnderTest.setInfinispanRemoteCacheManager(remoteCacheManager);
      objectUnderTest.setCacheName(TEST_CACHE_NAME);
      objectUnderTest.setBeanName(TEST_BEAN_NAME);
      objectUnderTest.afterPropertiesSet();

      final BasicCache<String, Object> cache = objectUnderTest.getObject();

      assertNotNull(
               "InfinispanNamedRemoteCacheFactoryBean should have produced a proper Infinispan cache. "
                        + "However, it produced a null Infinispan cache.", cache);
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.embedded.InfinispanNamedRemoteCacheFactoryBean#isSingleton()}.
    */
   @Test
   public final void infinispanNamedRemoteCacheFactoryBeanShouldDeclareItselfToBeSingleton() {
      final InfinispanNamedRemoteCacheFactoryBean<String, Object> objectUnderTest = new InfinispanNamedRemoteCacheFactoryBean<String, Object>();

      assertTrue(
               "InfinispanNamedRemoteCacheFactoryBean should declare itself to produce a singleton. However, it didn't.",
               objectUnderTest.isSingleton());
   }
}
