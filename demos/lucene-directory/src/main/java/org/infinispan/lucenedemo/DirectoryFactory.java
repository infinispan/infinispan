/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.lucenedemo;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.lucene.CacheKey;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility to create a Directory for the demo.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class DirectoryFactory {

   public static final String CACHE_NAME_FOR_INDEXES = "LuceneIndex";

   private static CacheManager manager = null;
   private static final Map<String, InfinispanDirectory> directories = new HashMap<String, InfinispanDirectory>();

   private static Cache<CacheKey, Object> buildCacheForIndexes() {
      return getCacheManager().getCache(CACHE_NAME_FOR_INDEXES);
   }

   public static synchronized CacheManager getCacheManager() {
      if (manager != null)
         return manager;
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      gc.setClusterName("infinispan-lucene-demo-cluster");
      gc.setTransportClass(JGroupsTransport.class.getName());
      Properties p = new Properties();

      // use the default that ships with Infinispan! - Manik Surtani, 7-Dec-2009
//      p.setProperty("configurationFile", "jgroups-configuration.xml");

      gc.setTransportProperties(p);

      Configuration config = new Configuration();
      config.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      config.setSyncCommitPhase(true);
      config.setSyncRollbackPhase(true);
      config.setTransactionManagerLookupClass(JBossStandaloneJTAManagerLookup.class.getName());
      config.setNumOwners(2);
      config.setL1CacheEnabled(true);
      config.setInvocationBatchingEnabled(true);
      config.setL1Lifespan(6000000);
      manager = new DefaultCacheManager(gc, config, false);
      return manager;
   }

   public static synchronized InfinispanDirectory getIndex(String indexName) {
      InfinispanDirectory dir = directories.get(indexName);
      if (dir == null) {
         dir = new InfinispanDirectory(buildCacheForIndexes(), indexName);
         directories.put(indexName, dir);
      }
      return dir;
   }

}
