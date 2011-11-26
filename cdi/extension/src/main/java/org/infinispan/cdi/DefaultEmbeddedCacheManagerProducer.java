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
package org.infinispan.cdi;

import org.infinispan.cdi.util.logging.Log;
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

/**
 * <p>The default embedded cache manager producer.</p>
 *
 * <p>By default the cache manager produced is an instance of {@link DefaultCacheManager} initialized with the default
 * configuration produced by the {@link DefaultEmbeddedCacheConfigurationProducer}. The embedded cache manager used by
 * default can be overridden by creating a producer which produces the new default cache manager. The new default
 * embedded cache manager produced must be qualified by {@link OverrideDefault} and have the scope
 * {link ApplicationScoped}.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultEmbeddedCacheManagerProducer {

   private static final Log log = LogFactory.getLog(DefaultEmbeddedCacheManagerProducer.class, Log.class);

   /**
    * Produces the default cache manager.
    *
    * @param providedDefaultEmbeddedCacheManager the provided default embedded cache manager.
    * @param defaultConfiguration the default configuration produced by the {@link DefaultEmbeddedCacheConfigurationProducer}.
    * @return the default cache manager used by the application.
    */
   @Produces
   @Default
   @ApplicationScoped
   public EmbeddedCacheManager getDefaultEmbeddedCacheManager(@OverrideDefault Instance<EmbeddedCacheManager> providedDefaultEmbeddedCacheManager, @Default Configuration defaultConfiguration) {
      if (!providedDefaultEmbeddedCacheManager.isUnsatisfied()) {
         log.tracef("Default embedded cache manager overridden by '%s'", providedDefaultEmbeddedCacheManager);
         return providedDefaultEmbeddedCacheManager.get();
      }
      return new DefaultCacheManager(defaultConfiguration);
   }

   /**
    * Stops the default cache manager when the corresponding instance is released.
    *
    * @param defaultEmbeddedCacheManager the default cache manager produced.
    */
   private void stopCacheManager(@Disposes @Default EmbeddedCacheManager defaultEmbeddedCacheManager) {
      defaultEmbeddedCacheManager.stop();
   }
}
