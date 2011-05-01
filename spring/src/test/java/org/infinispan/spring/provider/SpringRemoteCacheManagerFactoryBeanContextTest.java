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

package org.infinispan.spring.provider;

import static org.testng.AssertJUnit.assertNotNull;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerFactoryBeanContextTest", groups = "unit")
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/provider/SpringRemoteCacheManagerFactoryBeanContextTest.xml")
public class SpringRemoteCacheManagerFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "springRemoteCacheManagerWithDefaultConfiguration";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME = "springRemoteCacheManagerConfiguredFromConfigurationPropertiesFile";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME = "springRemoteCacheManagerConfiguredUsingConfigurationProperties";

   private static final String SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "springRemoteCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateARemoteCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final SpringRemoteCacheManager springRemoteCacheManagerWithDefaultConfiguration = this.applicationContext
               .getBean(SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                        SpringRemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a SpringRemoteCacheManager with default settings having bean name = \""
                        + SPRING_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               springRemoteCacheManagerWithDefaultConfiguration);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationFileIfConfigurationFileLocationGiven() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredFromConfigurationFile = this.applicationContext
               .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME,
                        SpringRemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a SpringRemoteCacheManager configured from configuration file having bean name = \""
                        + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME
                        + "\". However, it doesn't.",
               springRemoteCacheManagerConfiguredFromConfigurationFile);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingConfigurationPropertiesSetInApplicationContext() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredUsingConfigurationProperties = this.applicationContext
               .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME,
                        SpringRemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a SpringRemoteCacheManager configured using configuration properties set in application context having bean name = \""
                        + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_CONFIGURATION_PROPERTIES_BEAN_NAME
                        + "\". However, it doesn't.",
               springRemoteCacheManagerConfiguredUsingConfigurationProperties);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingSettersIfPropertiesAreDefined() {
      final SpringRemoteCacheManager springRemoteCacheManagerConfiguredUsingSetters = this.applicationContext
               .getBean(SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                        SpringRemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a SpringRemoteCacheManager configured using properties having bean name = \""
                        + SPRING_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                        + "\". However, it doesn't.",
               springRemoteCacheManagerConfiguredUsingSetters);
   }
}
