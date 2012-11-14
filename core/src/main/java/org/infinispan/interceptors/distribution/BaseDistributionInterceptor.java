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
package org.infinispan.interceptors.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Base class for distribution of entries across a cluster.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dan@infinispan.org>
 * @since 4.0
 */
public abstract class BaseDistributionInterceptor extends BaseRpcInterceptor {
   protected DistributionManager dm;
   protected CommandsFactory cf;
   protected DataContainer dataContainer;
   protected EntryFactory entryFactory;
   protected LockManager lockManager;
   protected ClusteringDependentLogic cdl;

   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   protected static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager,
                                  CommandsFactory cf, DataContainer dataContainer, EntryFactory entryFactory,
                                  LockManager lockManager, ClusteringDependentLogic cdl) {
      this.dm = distributionManager;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
      this.lockManager = lockManager;
      this.cdl = cdl;
   }

   protected boolean needsRemoteGet(InvocationContext ctx, AbstractDataCommand command, boolean retvalCheck) {
      final CacheEntry entry;
      return retvalCheck
            && !command.hasFlag(Flag.CACHE_MODE_LOCAL)
            && !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !command.hasFlag(Flag.IGNORE_RETURN_VALUES)
            && ((entry = ctx.lookupEntry(command.getKey())) == null || entry.isNull() || entry.isLockPlaceholder());   //todo [anistor] this condition seems wrong
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return handleWriteCommand(ctx, command,
                                new MultipleKeysRecipientGenerator(command.getMap().keySet()), true, false);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false, false);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false, false);
   }

   protected boolean shouldFetchFromRemote(InvocationContext ctx, Object key) {
      DataLocality locality = dm.getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key) ? DataLocality.LOCAL : DataLocality.NOT_LOCAL;
      boolean shouldFetch = ctx.isOriginLocal() && !locality.isLocal() && !dataContainer.containsKey(key);
      if (!shouldFetch) {
         log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
      }
      return shouldFetch;
   }

   protected abstract Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable;

   /**
    * If a single owner has been configured and the target for the key is the local address, it returns true.
    */
   protected boolean isSingleOwnerAndLocal(RecipientGenerator recipientGenerator) {
      List<Address> recipients;
      return cacheConfiguration.clustering().hash().numOwners() == 1
            && (recipients = recipientGenerator.generateRecipients()) != null
            && recipients.size() == 1
            && recipients.get(0).equals(rpcManager.getTransport().getAddress());
   }

   interface KeyGenerator {
      Collection<Object> getKeys();
   }

   interface RecipientGenerator extends KeyGenerator {
      List<Address> generateRecipients();
   }

   class SingleKeyRecipientGenerator implements RecipientGenerator {
      final Object key;
      final Set<Object> keys;
      List<Address> recipients = null;

      SingleKeyRecipientGenerator(Object key) {
         this.key = key;
         keys = Collections.singleton(key);
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) recipients = dm.locate(key);
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }

   class MultipleKeysRecipientGenerator implements RecipientGenerator {

      final Collection<Object> keys;
      List<Address> recipients = null;

      MultipleKeysRecipientGenerator(Collection<Object> keys) {
         this.keys = keys;
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) {
            Set<Address> addresses = dm.locateAll(keys);
            recipients = Immutables.immutableListConvert(addresses);
         }
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }

}
