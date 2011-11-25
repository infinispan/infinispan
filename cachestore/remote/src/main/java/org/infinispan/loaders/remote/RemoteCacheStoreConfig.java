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
package org.infinispan.loaders.remote;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.infinispan.CacheException;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configuration for RemoteCacheStore.
 * <p/>
 * Parameters:
 * <ul>
 * <li>HotRodClientPropertiesFile-the file that contains the configuration of Hot Rod client. See <a href="http://community.jboss.org/wiki/JavaHotRodclient">Hotrod Java Client</a>
 *     for more details on the Hot Rod client. 
 * <li>remoteCacheName-the name of the remote cache in the remote infinispan cluster, to which to connect to</li>
 * <li>UseDefaultRemoteCache-if set to true, the default remote cache will be used, as obtained by {@link org.infinispan.manager.CacheContainer#getCache()}.
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheStoreConfig extends AbstractCacheStoreConfig {

   private volatile String remoteCacheName;
   private static final Log log = LogFactory.getLog(RemoteCacheStoreConfig.class);
   private final Properties hotRodClientProperties = new Properties();
   private ExecutorFactory asyncExecutorFactory = null;

   public RemoteCacheStoreConfig() {
      setCacheLoaderClassName(RemoteCacheStore.class.getName());
   }

   public void setRemoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
   }

   public String getRemoteCacheName() {
      return remoteCacheName;
   }

   public void setUseDefaultRemoteCache(boolean useDefaultRemoteCache) {
      if (useDefaultRemoteCache) {
         setRemoteCacheName(BasicCacheContainer.DEFAULT_CACHE_NAME);
      }
   }

   public boolean isUseDefaultRemoteCache() {
      return CacheContainer.DEFAULT_CACHE_NAME.equals(getRemoteCacheName());
   }

   public Properties getHotRodClientProperties() {
      return hotRodClientProperties;
   }

   public void setHotRodClientProperties(Properties props) {
      hotRodClientProperties.putAll(props);
   }

   public ExecutorFactory getAsyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   public void setAsyncExecutorFactory(ExecutorFactory asyncExecutorFactory) {
      this.asyncExecutorFactory = asyncExecutorFactory;
   }

   public void setHotRodClientPropertiesFile(String hotRodClientPropertiesFile) {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream inputStream = fileLookup.lookupFile(hotRodClientPropertiesFile, getClassLoader());
      try {
         hotRodClientProperties.load(inputStream);
      } catch (IOException e) {
         log.error("Issues while loading properties from file " + hotRodClientPropertiesFile, e);
         throw new CacheException(e);
      } finally {
         Util.close(inputStream);
      }
   }
}
