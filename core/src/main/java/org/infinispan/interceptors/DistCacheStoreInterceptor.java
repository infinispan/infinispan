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

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.Map;

/**
 * Cache store interceptor specific for the distribution cache mode. Put operations has been modified in such way that
 * if they put operation is the result of an L1 put, storing in the cache store is ignore. This is done so that immortal
 * entries that get converted into mortal ones when putting into L1 don't get propagated to the cache store.
 * <p/>
 * Secondly, in a replicated environment where a shared cache store is used, the node in which the cache operation is
 * executed is the one responsible for interacting with the cache. This doesn't work with distributed mode and instead,
 * in a shared cache store situation, the first owner of the key is the one responsible for storing it.
 * <p/>
 * In the particular case of putAll(), individual keys are checked and if a shared cache store environment has been
 * configured, only the first owner of that key will actually store it to the cache store. In a unshared environment
 * though, only those nodes that are owners of the key would store it to their local cache stores.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class DistCacheStoreInterceptor extends CacheStoreInterceptor {
   DistributionManager dm;
   Transport transport;
   Address address;

   private static final Log log = LogFactory.getLog(DistCacheStoreInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void inject(DistributionManager dm, Transport transport) {
      this.dm = dm;
      this.transport = transport;
   }

   @Start(priority = 25)
   // after the distribution manager!
   private void setAddress() {
      this.address = transport.getAddress();
   }

   // ---- WRITE commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (skip(ctx, key) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;
      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();
      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      if (skip(ctx) || ctx.isInTxScope()) return returnValue;

      Map<Object, Object> map = command.getMap();
      for (Object key : map.keySet()) {
         if (!skipKey(key)) {
            InternalCacheEntry se = getStoredEntry(key, ctx);
            store.store(se);
            log.tracef("Stored entry %s under key %s", se, key);
         }
      }
      if (getStatisticsEnabled()) cacheStores.getAndAdd(map.size());
      return returnValue;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (!skip(ctx, key) && !ctx.isInTxScope() && command.isSuccessful()) {
         boolean resp = store.remove(key);
         log.tracef("Removed entry under key %s and got response %s from CacheStore", key, resp);
      }
      return retval;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);
      Object key = command.getKey();
      if (skip(ctx, key) || ctx.isInTxScope() || !command.isSuccessful()) return returnValue;

      InternalCacheEntry se = getStoredEntry(key, ctx);
      store.store(se);
      log.tracef("Stored entry %s under key %s", se, key);
      if (getStatisticsEnabled()) cacheStores.incrementAndGet();

      return returnValue;
   }

   /**
    * Method that skips invocation if: - No store defined or, - The context contains Flag.SKIP_CACHE_STORE or, - The
    * store is a shared one and node storing the key is not the 1st owner of the key or, - This is an L1 put operation.
    */
   private boolean skip(InvocationContext ctx, Object key) {
      return skip(ctx) || skipKey(key);
   }

   /**
    * Method that skips invocation if: - No store defined or, - The context contains Flag.SKIP_CACHE_STORE or,
    */
   private boolean skip(InvocationContext ctx) {
      if (store == null) {
         log.trace("Skipping cache store because the cache loader does not implement CacheStore");
         return true;
      }
      if (ctx.hasFlag(Flag.SKIP_CACHE_STORE)) {
         log.trace("Skipping cache store since the call contain a skip cache store flag");
         return true;
      }
      if (loaderConfig.isShared() && ctx.hasFlag(Flag.SKIP_SHARED_CACHE_STORE)) {
         log.trace("Skipping cache store since it is shared and the call contain a skip shared cache store flag");
      }
      return false;
   }

   /**
    * Method that skips invocation if: - The store is a shared one and node storing the key is not the 1st owner of the
    * key or, - This is an L1 put operation.
    */
   @Override
   protected boolean skipKey(Object key) {
      List<Address> addresses = dm.locate(key);
      if (loaderConfig.isShared()) {
         if (!isFirstOwner(addresses)) {
            log.trace("Skipping cache store since the cache loader is shared " +
                  "and the caller is not the first owner of the key");
            return true;
         }
      } else {
         if (isL1Put(addresses)) {
            log.trace("Skipping cache store since this is an L1 put");
            return true;
         }
      }
      return false;
   }

   private boolean isL1Put(List<Address> addresses) {
      if (address == null) throw new NullPointerException("Local address cannot be null!");
      return !addresses.contains(address);
   }

   private boolean isFirstOwner(List<Address> addresses) {
      if (address == null) throw new NullPointerException("Local address cannot be null!");
      return addresses.get(0).equals(address);
   }

}
