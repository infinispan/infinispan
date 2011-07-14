/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.demo;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Builds CacheManager given Infinispan configuration and transport file.
 */
public class CacheBuilder {

   private EmbeddedCacheManager cacheManager;

   public CacheBuilder(String ispnConfigFile) throws IOException {
      cacheManager = new DefaultCacheManager(findConfigFile(ispnConfigFile));
   }

   public EmbeddedCacheManager getCacheManager() {
      return this.cacheManager;
   }

   private String findConfigFile(String configFile) {
      FileLookup fl = FileLookupFactory.newInstance();
      if (configFile != null) {
         InputStream inputStream = fl.lookupFile(configFile, Thread.currentThread().getContextClassLoader());
         try {
            if (inputStream != null)
               return configFile;
         } finally {
            Util.close(inputStream);
         }
      }

      return null;
   }
}
