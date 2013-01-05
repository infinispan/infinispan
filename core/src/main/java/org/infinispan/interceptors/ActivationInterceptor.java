/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EvictionConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ActivationInterceptor extends CacheLoaderInterceptor {

   private static final Log log = LogFactory.getLog(ActivationInterceptor.class);

   private Configuration cfg;
   private boolean isManualEviction;
   private ActivationManager activationManager;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(Configuration cfg, ActivationManager activationManager) {
      this.cfg = cfg;
      this.activationManager = activationManager;
   }

   @Start(priority = 15)
   @SuppressWarnings("unused")
   public void start() {
      // Treat caches configured with manual eviction differently.
      // These caches require activation at the interceptor level.
      EvictionConfiguration evictCfg = cfg.eviction();
      isManualEviction = evictCfg.strategy() == EvictionStrategy.NONE
            || evictCfg.maxEntries() < 0;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object retval = super.visitPutKeyValueCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());

      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = super.visitRemoveCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object retval = super.visitReplaceCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
   }


   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object retval = super.visitPutMapCommand(ctx, command);
      removeFromStoreIfNeeded(command.getMap().keySet().toArray());
      return retval;
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retval = super.visitGetKeyValueCommand(ctx, command);
      removeFromStoreIfNeeded(command.getKey());
      return retval;
   }

   @Override
   protected void sendNotification(Object key, Object value, boolean pre, InvocationContext ctx) {
      super.sendNotification(key, value, pre, ctx);
      notifier.notifyCacheEntryActivated(key, value, pre, ctx);
   }

   private void removeFromStoreIfNeeded(Object... keys) {
      if (enabled && isManualEviction) {
         for (Object key: keys)
            activationManager.activate(key);
      }
   }

}


