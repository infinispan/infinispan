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
package org.infinispan.cdi.test.cachemanager.xml;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.cdi.OverrideDefault;
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.solder.resourceLoader.Resource;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.io.InputStream;

/**
 * Creates a number of caches, based on some external mechanism.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Associates the "very-large" cache (configured below) with the qualifier {@link VeryLarge}.</p>
    *
    * <p>The default configuration defined in "infinispan.xml" will be used.</p>
    */
   @Produces
   @ConfigureCache("very-large")
   @VeryLarge
   public Configuration veryLargeConfiguration;

   /**
    * Associates the "quick-very-large" cache (configured below) with the qualifier {@link Quick}.
    */
   @Produces
   @ConfigureCache("quick-very-large")
   @Quick
   public Configuration quickVeryLargeConfiguration;

   /**
    * Overrides the default cache manager.
    */
   @Produces
   @OverrideDefault
   @ApplicationScoped
   public EmbeddedCacheManager defaultCacheManager(@Resource("infinispan.xml") InputStream xml) throws IOException {
      EmbeddedCacheManager externalCacheContainerManager = new DefaultCacheManager(xml);

      externalCacheContainerManager.defineConfiguration("quick-very-large", new Configuration().fluent()
            .expiration().wakeUpInterval(1l)
            .build());

      return externalCacheContainerManager;
   }
}
