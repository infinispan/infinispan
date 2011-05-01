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

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * <p>
 * Test {@link InfinispanNamedRemoteCacheFactoryBean} deployed in a Spring application context.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath:/org/infinispan/spring/support/remote/InfinispanNamedRemoteCacheFactoryBeanContextTest.xml")
// @Test(testName = "spring.support.remote.InfinispanNamedRemoteCacheFactoryBeanContextTest", groups
// = "functional")
public class InfinispanNamedRemoteCacheFactoryBeanContextTest extends
         AbstractTestNGSpringContextTests {

   private static final String INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME = "infinispanNamedRemoteCacheWithoutFurtherConfiguration";

   // @Test
   public final void shouldCreateARemoteCacheWithDefaultSettingsIfNoFurtherConfigurationGiven() {
      final Cache<Object, Object> infinispanNamedRemoteCacheWithoutFurtherConfiguration = this.applicationContext
               .getBean(INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME,
                        Cache.class);

      assertNotNull(
               "Spring application context should contain a named Infinispan cache having bean name = \""
                        + INFINISPAN_NAMED_REMOTE_CACHE_WITHOUT_FURTHER_CONFIGURATION_BEAN_NAME
                        + "\". However, it doesn't.",
               infinispanNamedRemoteCacheWithoutFurtherConfiguration);
   }
}
