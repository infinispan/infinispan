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

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

/**
 * <p>
 * Test {@link InfinispanRemoteCacheManagerFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/remote/InfinispanRemoteCacheManagerFactoryBeanContextTest.xml")
@Test(testName = "spring.support.remote.InfinispanRemoteCacheManagerFactoryBeanContextTest", groups = "unit")
public class InfinispanRemoteCacheManagerFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME = "infinispanRemoteCacheManagerWithDefaultConfiguration";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredFromConfigurationPropertiesFile";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredFromConfigurationProperties";

   private static final String INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME = "infinispanRemoteCacheManagerConfiguredUsingSetters";

   @Test
   public final void shouldCreateARemoteCacheManagerWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerWithDefaultConfiguration = this.applicationContext
               .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME,
                        RemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a RemoteCacheManager with default settings having bean name = \""
                        + INFINISPAN_REMOTE_CACHE_MANAGER_WITH_DEFAULT_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanRemoteCacheManagerWithDefaultConfiguration);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationPropertiesFileIfConfigurationPropertiesFileLocationGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredFromConfigurationFile = this.applicationContext
               .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME,
                        RemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a RemoteCacheManager configured from configuration properties file having bean name = \""
                        + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_FILE_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanRemoteCacheManagerConfiguredFromConfigurationFile);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredFromConfigurationPropertiesIfConfigurationPropertiesGiven() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredFromConfigurationProperties = this.applicationContext
               .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME,
                        RemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a RemoteCacheManager configured from configuration properties having bean name = \""
                        + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_FROM_CONFIGURATION_PROPERTIES_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanRemoteCacheManagerConfiguredFromConfigurationProperties);
   }

   @Test
   public final void shouldCreateARemoteCacheManagerConfiguredUsingSettersIfPropertiesAreDefined() {
      final RemoteCacheManager infinispanRemoteCacheManagerConfiguredUsingSetters = this.applicationContext
               .getBean(INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME,
                        RemoteCacheManager.class);

      assertNotNull(
               "Spring application context should contain a SpringRemoteCacheManager configured using properties having bean name = \""
                        + INFINISPAN_REMOTE_CACHE_MANAGER_CONFIGURED_USING_SETTERS_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanRemoteCacheManagerConfiguredUsingSetters);
   }
}
