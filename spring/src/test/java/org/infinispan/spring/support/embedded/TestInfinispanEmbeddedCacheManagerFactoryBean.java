/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.spring.support.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Bean that creates cache manager instances produced
 * by the test cache manager factory.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class TestInfinispanEmbeddedCacheManagerFactoryBean extends InfinispanEmbeddedCacheManagerFactoryBean {

   @Override
   protected EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      return TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(InputStream is) throws IOException {
      return TestCacheManagerFactory.fromStream(is);
   }

}
