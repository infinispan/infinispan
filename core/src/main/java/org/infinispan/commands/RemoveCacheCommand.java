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

package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.Util;

/**
 * Command to stop a cache and remove all its contents from both
 * memory and any backing store.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class RemoveCacheCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 18;

   private EmbeddedCacheManager cacheManager;
   private GlobalComponentRegistry registry;
   private CacheLoaderManager cacheLoaderManager;

   private RemoveCacheCommand() {
      super(null); // For command id uniqueness test
   }

   public RemoveCacheCommand(String cacheName, EmbeddedCacheManager cacheManager,
         GlobalComponentRegistry registry, CacheLoaderManager cacheLoaderManager) {
      super(cacheName);
      this.cacheManager = cacheManager;
      this.registry = registry;
      this.cacheLoaderManager = cacheLoaderManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // To avoid reliance of a thread local flag, get a reference for the
      // cache store to be able to clear it after cache has stopped.
      CacheStore store = cacheLoaderManager.getCacheStore();
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      CacheJmxRegistration jmx = cache.getAdvancedCache().getComponentRegistry().getComponent(CacheJmxRegistration.class);
      cache.stop();

      // After stopping the cache, clear it
      if (store != null)
         store.clear();

      // And see if we need to remove it from JMX
      if (jmx != null) {
         jmx.unregisterCacheMBean();
      }

      registry.removeCache(cacheName);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      // No parameters
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
