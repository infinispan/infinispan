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
package org.jboss.seam.infinispan.test.cacheManager.xml;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.infinispan.CacheContainerManager;
import org.jboss.seam.solder.resourceLoader.Resource;

import javax.enterprise.inject.Specializes;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@Specializes
public class ExternalCacheContainerManager extends CacheContainerManager {

   /**
    * Create the Cache Container from XML,
    *
    * @param xml
    * @return
    * @throws IOException
    */
   public static CacheContainer createCacheContainer(InputStream xml)
         throws IOException {
      // Create the cache container from the XML config, and associate with the
      // producer fields
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(xml);

      // Define the very-large and quick-very-large configuration, based on the
      // defaults
      cacheManager.defineConfiguration("very-large", cacheManager
            .getDefaultConfiguration().clone());

      Configuration quickVeryLargeConfiguration = cacheManager
            .getDefaultConfiguration().clone();
      quickVeryLargeConfiguration.fluent()
            .eviction()
            .wakeUpInterval(1l);
      cacheManager.defineConfiguration("quick-very-large",
                                       quickVeryLargeConfiguration);
      return cacheManager;
   }

   // Constructor for proxies only
   protected ExternalCacheContainerManager() {
   }

   @Inject
   public ExternalCacheContainerManager(@Resource("infinispan.xml") InputStream xml) throws IOException {
      super(createCacheContainer(xml));
   }

}
