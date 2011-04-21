/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.demos.gridfs;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import java.io.IOException;

/**
 * A bootstrapping startup listener which creates and holds a cache instance
 */
public class CacheManagerHolder extends HttpServlet {

   private static final Log log = LogFactory.getLog(CacheManagerHolder.class);

   private static final String CFG_PROPERTY = "infinispan.config";
   private static final String DATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.data";
   private static final String METADATA_CACHE_NAME_PROPERTY = "infinispan.gridfs.cache.metadata";

   public static CacheContainer cacheContainer;
   public static String dataCacheName, metadataCacheName;

   @Override
   public void init(ServletConfig cfg) throws ServletException {
      super.init(cfg);
      String cfgFile = cfg.getInitParameter(CFG_PROPERTY);
      if (cfgFile == null)
         cacheContainer = new DefaultCacheManager();
      else {
         try {
            cacheContainer = new DefaultCacheManager(cfgFile);
         } catch (IOException e) {
            log.error("Unable to start cache manager with config file " + cfgFile + ".  Using DEFAULTS!");
            cacheContainer = new DefaultCacheManager();
         }
      }

      dataCacheName = cfg.getInitParameter(DATA_CACHE_NAME_PROPERTY);
      metadataCacheName = cfg.getInitParameter(METADATA_CACHE_NAME_PROPERTY);
   }
}
