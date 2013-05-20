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
package org.infinispan.commands.remote;

import org.infinispan.Metadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Equivalence;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * Issues a remote get call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClusteredGetCommand extends BaseRpcCommand implements FlagAffectedCommand {

   public static final byte COMMAND_ID = 16;
   private static final Log log = LogFactory.getLog(ClusteredGetCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Object key;

   private InvocationContextContainer icc;
   private CommandsFactory commandsFactory;
   private InterceptorChain invoker;
   private boolean acquireRemoteLock;
   private GlobalTransaction gtx;
   private Set<Flag> flags;

   private DistributionManager distributionManager;
   private TransactionTable txTable;
   private InternalEntryFactory entryFactory;
   private int topologyId;
   private Equivalence keyEquivalence;

   private ClusteredGetCommand() {
      super(null); // For command id uniqueness test
   }

   public ClusteredGetCommand(String cacheName) {
      super(cacheName);
   }

   public ClusteredGetCommand(Object key, String cacheName, Set<Flag> flags,
         boolean acquireRemoteLock, GlobalTransaction gtx, Equivalence keyEquivalence) {
      super(cacheName);
      this.key = key;
      this.flags = flags;
      this.acquireRemoteLock = acquireRemoteLock;
      this.gtx = gtx;
      this.keyEquivalence = keyEquivalence;
      if (acquireRemoteLock && (gtx == null))
         throw new IllegalArgumentException("Cannot have null tx if we need to acquire locks");
   }

   public void initialize(InvocationContextContainer icc, CommandsFactory commandsFactory, InternalEntryFactory entryFactory,
         InterceptorChain interceptorChain, DistributionManager distributionManager, TransactionTable txTable,
         Equivalence keyEquivalence) {
      this.distributionManager = distributionManager;
      this.icc = icc;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.txTable = txTable;
      this.entryFactory = entryFactory;
      this.keyEquivalence = keyEquivalence;
   }

   /**
    * Invokes a logical "get(key)" on a remote cache and returns results.
    *
    * @param context invocation context, ignored.
    * @return returns an <code>CacheEntry</code> or null, if no entry is found.
    */
   @Override
   public InternalCacheValue perform(InvocationContext context) throws Throwable {
      acquireLocksIfNeeded();
      if (distributionManager != null && distributionManager.isAffectedByRehash(key)) return null;
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      Set<Flag> commandFlags = EnumSet.of(Flag.SKIP_REMOTE_LOOKUP, Flag.CACHE_MODE_LOCAL);
      if (this.flags != null) commandFlags.addAll(this.flags);
      GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, commandFlags, true);
      InvocationContext invocationContext = icc.createRemoteInvocationContextForCommand(command, getOrigin());
      CacheEntry cacheEntry = (CacheEntry) invoker.invoke(invocationContext, command);
      if (cacheEntry == null) {
         if (trace) log.trace("Did not find anything, returning null");
         return null;
      }
      //this might happen if the value was fetched from a cache loader
      if (cacheEntry instanceof MVCCEntry) {
         if (trace) log.trace("Handling an internal cache entry...");
         MVCCEntry mvccEntry = (MVCCEntry) cacheEntry;
         return entryFactory.createValue(mvccEntry);
      } else {
         InternalCacheEntry internalCacheEntry = (InternalCacheEntry) cacheEntry;
         return internalCacheEntry.toInternalCacheValue();
      }
   }

   private void acquireLocksIfNeeded() throws Throwable {
      if (acquireRemoteLock) {
         LockControlCommand lockControlCommand = commandsFactory.buildLockControlCommand(key, flags, gtx);
         lockControlCommand.init(invoker, icc, txTable);
         lockControlCommand.perform(null);
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, flags, acquireRemoteLock, gtx};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      key = args[i++];
      flags = (Set<Flag>) args[i++];
      acquireRemoteLock = (Boolean) args[i++];
      gtx = (GlobalTransaction) args[i];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteredGetCommand that = (ClusteredGetCommand) o;

      return !(key != null ?
         !(keyEquivalence != null ? keyEquivalence.equals(key, that.key) : key.equals(that.key))
         : that.key != null);
   }

   @Override
   public int hashCode() {
      int result;
      result = (key != null
          ? (keyEquivalence != null ? keyEquivalence.hashCode(key) : key.hashCode())
          : 0);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("ClusteredGetCommand{key=")
         .append(key)
         .append(", flags=").append(flags)
         .append("}")
         .toString();
   }

   public Object getKey() {
      return key;
   }

   @Override
   public Set<Flag> getFlags() {
      return flags;
   }

   @Override
   public void setFlags(Set<Flag> flags) {
      this.flags = flags;
   }

   @Override
   public void setFlags(Flag... flags) {
      if (flags == null || flags.length == 0) return;
      if (this.flags == null)
         this.flags = EnumSet.copyOf(Arrays.asList(flags));
      else
         this.flags.addAll(Arrays.asList(flags));
   }

   @Override
   public boolean hasFlag(Flag flag) {
      return flags != null && flags.contains(flag);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitUnknownCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public Metadata getMetadata() {
      return null;
   }

}
