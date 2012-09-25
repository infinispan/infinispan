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
package org.infinispan.cdi.test;

import org.infinispan.cdi.OverrideDefault;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

/**
 * <p>The alternative default {@link EmbeddedCacheManager} producer for the test environment.</p>
 *
 * @author Galder Zamarreño
 */
public class DefaultTestEmbeddedCacheManagerProducer {

   /**
    * Produces the default embedded cache manager.
    *
    * @param providedDefaultEmbeddedCacheManager the provided default embedded cache manager.
    * @param defaultConfiguration the default configuration produced by the {@link DefaultTestEmbeddedCacheManagerProducer}.
    * @return the default embedded cache manager used by the application.
    */
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(@OverrideDefault Instance<EmbeddedCacheManager> providedDefaultEmbeddedCacheManager, Configuration defaultConfiguration) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(defaultConfiguration);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   /**
    * Stops the default embedded cache manager when the corresponding instance is released.
    *
    * @param defaultEmbeddedCacheManager the default embedded cache manager.
    */
   private void stopCacheManager(@Disposes EmbeddedCacheManager defaultEmbeddedCacheManager) {
      defaultEmbeddedCacheManager.stop();
   }
}
