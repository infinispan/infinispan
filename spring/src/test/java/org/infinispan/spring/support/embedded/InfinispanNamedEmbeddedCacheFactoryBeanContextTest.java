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

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link InfinispanNameEmbeddedCacheFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/embedded/InfinispanNamedEmbeddedCacheFactoryBeanContextTest.xml")
@Test(testName = "spring.support.embedded.InfinispanNamedEmbeddedCacheFactoryBeanContextTest", groups = "unit")
public class InfinispanNamedEmbeddedCacheFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME = "infinispanNamedEmbeddedCacheWithoutFurtherConfiguration";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeNONE";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeDEFAULT";

   private static final String INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME = "infinispanNamedEmbeddedCacheConfiguredUsingModeNAMED";

   @Test
   public final void shouldCreateAnEmbeddedCacheWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheWithoutFurtherConfiguration = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanNamedEmbeddedCacheWithoutFurtherConfiguration);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeNONE() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNone = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NONE_BEAN_NAME
                        + "\" that has been configured using configuration mode NONE. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNone);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeDEFAULT() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeDefault = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_DEFAULT_BEAN_NAME
                        + "\" that has been configured using configuration mode DEFAULT. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeDefault);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheConfiguredUsingConfigurationModeNAMED() {
      final Cache<Object, Object> infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNamed = this.applicationContext
               .getBean(INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_EMBEDDED_CACHE_CONFIGURED_USING_MODE_NAMED_BEAN_NAME
                        + "\" that has been configured using configuration mode NAMED. However, it doesn't.",
               infinispanNamedEmbeddedCacheConfiguredUsingConfigurationModeNamed);
   }
}
