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

import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * <p>This producer is responsible to produce the default cache manager used in the application.</p>
 * <p>By default the cache manager used is a {@link DefaultCacheManager} with the default configuration. If you want to
 * provide your own default cache manager follow this steps:
 * <ol>
 *    <li>Extend this bean</li>
 *    <li>Add {@linkplain javax.enterprise.inject.Specializes @Specializes} annotation on your class</li>
 *    <li>Override the {@link DefaultCacheManagerProducer#getDefaultCacheManager(org.infinispan.config.Configuration)}
 *    method.</li>
 * </ol></p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultCacheManagerProducer {
   /**
    * <p>Produces the default cache manager used.<p>
    * <p>Note: there is only one instance of the default cache manager per application.</p>
    *
    * @param defaultConfiguration the default cache configuration produced by the {@link DefaultCacheConfigurationProducer}.
    * @return the default cache manager used by the application.
    */
   @Produces
   @Default
   @ApplicationScoped
   public EmbeddedCacheManager getDefaultCacheManager(@Default Configuration defaultConfiguration) {
      return new DefaultCacheManager(defaultConfiguration);
   }

   /**
    * This method is responsible to stop the default cache manager when the corresponding instance is released.
    *
    * @param defaultCacheManager the default cache manager produced.
    */
   private void stopCacheManager(@Disposes @Default EmbeddedCacheManager defaultCacheManager) {
      defaultCacheManager.stop();
   }
}
