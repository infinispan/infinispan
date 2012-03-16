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

import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

/**
 * <p>
 * Test {@link InfinispanEmbeddedCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/embedded/InfinispanEmbeddedCacheManagerFactoryBeanContextTest.xml")
@Test(testName = "spring.support.embedded.InfinispanEmbeddedCacheManagerFactoryBeanContextTest", groups = "functional")
public class InfinispanEmbeddedCacheManagerFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "infinispanEmbeddedCacheManagerWithDefaultConfiguration";

   private static final String INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME = "infinispanEmbeddedCacheManagerConfiguredFromConfigurationFile";

   private static final String INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "infinispanEmbeddedCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final EmbeddedCacheManager infinispanEmbeddedCacheManagerWithDefaultConfiguration = this.applicationContext
               .getBean(INFINISPAN_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                        EmbeddedCacheManager.class);

      assertNotNull(
               "Spring application context should contain a EmbeddedCacheManager with default settings having bean name = \""
                        + INFINISPAN_EMBEDDED_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanEmbeddedCacheManagerWithDefaultConfiguration);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerConfiguredFromConfigurationFileIfConfigurationFileLocationGiven() {
      final EmbeddedCacheManager infinispanEmbeddedCacheManagerConfiguredFromConfigurationFile = this.applicationContext
               .getBean(INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME,
                        EmbeddedCacheManager.class);

      assertNotNull(
               "Spring application context should contain a EmbeddedCacheManager configured from configuration file having bean name = \""
                        + INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_FILE_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanEmbeddedCacheManagerConfiguredFromConfigurationFile);
   }

   @Test
   public final void shouldCreateAnEmbeddedCacheManagerConfiguredUsingSettersIfPropertiesAreDefined() {
      final EmbeddedCacheManager infinispanEmbeddedCacheManagerConfiguredUsingSetters = this.applicationContext
               .getBean(INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                        EmbeddedCacheManager.class);

      assertNotNull(
               "Spring application context should contain a EmbeddedCacheManager configured using properties having bean name = \""
                        + INFINISPAN_EMBEDDED_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanEmbeddedCacheManagerConfiguredUsingSetters);
   }
}
