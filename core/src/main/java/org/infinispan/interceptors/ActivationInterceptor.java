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
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.concurrent.atomic.AtomicLong;

@MBean(objectName = "Activation", description = "Component that handles activating entries that have been passivated to a CacheStore by loading them into memory.")
public class ActivationInterceptor extends CacheLoaderInterceptor {

   private final AtomicLong activations = new AtomicLong(0);
   private CacheStore store;

   private static final Log log = LogFactory.getLog(ActivationInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Start(priority = 15)
   public void setCacheStore() {
      store = clm == null ? null : clm.getCacheStore();
      if (store == null) {
         throw new ConfigurationException("passivation can only be used with a CacheLoader that implements CacheStore!");
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object retval = super.visitPutKeyValueCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = super.visitRemoveCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object retval = super.visitReplaceCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }


   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object retval = super.visitPutMapCommand(ctx, command);
      removeFromStore(command.getMap().keySet().toArray());
      return retval;
   }

   // read commands

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retval = super.visitGetKeyValueCommand(ctx, command);
      removeFromStore(command.getKey());
      return retval;
   }

   private void removeFromStore(Object... keys) throws CacheLoaderException {
      if (!clm.isShared()) {
         for (Object k : keys) {
            if (store.remove(k) && getStatisticsEnabled()) {
               activations.incrementAndGet();
            }
         }
      } else {
         if (trace) log.trace("CacheStore is shared; will not remove from store when passivating!");
      }
   }

   @Override
   protected void sendNotification(Object key, Object value, boolean pre, InvocationContext ctx) {
      super.sendNotification(key, value, pre, ctx);
      notifier.notifyCacheEntryActivated(key, value, pre, ctx);
   }

   @ManagedAttribute(description = "Number of activation events")
   @Metric(displayName = "Number of cache entries activated", measurementType = MeasurementType.TRENDSUP)
   public String getActivations() {
      if (!getStatisticsEnabled()) {
         return "N/A";
      }
      return String.valueOf(activations.get());
   }

   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset statistics")
   public void resetStatistics() {
      super.resetStatistics();
      activations.set(0);
   }
}


