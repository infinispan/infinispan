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
package org.infinispan.commands.control;

import org.infinispan.Metadata;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * LockControlCommand is a command that enables distributed locking across infinispan nodes.
 * <p/>
 * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class LockControlCommand extends AbstractTransactionBoundaryCommand implements FlagAffectedCommand {

   private static final Log log = LogFactory.getLog(LockControlCommand.class);

   public static final int COMMAND_ID = 3;

   private List<Object> keys;
   private boolean unlock = false;
   private Set<Flag> flags;

   private LockControlCommand() {
      super(null); // For command id uniqueness test
   }

   public LockControlCommand(String cacheName) {
      super(cacheName);
   }

   public LockControlCommand(Collection<Object> keys, String cacheName, Set<Flag> flags, GlobalTransaction gtx) {
      super(cacheName);
      if (keys != null) {
         //building defensive copies is here in order to support replaceKey operation
         this.keys = new ArrayList<Object>(keys);
      } else {
         this.keys = InfinispanCollections.emptyList();
      }
      this.flags = flags;
      this.globalTx = gtx;
   }

   public LockControlCommand(Object key, String cacheName, Set<Flag> flags, GlobalTransaction gtx) {
      this(cacheName);
      this.keys = new ArrayList<Object>(1);
      this.keys.add(key);
      this.flags = flags;
      this.globalTx = gtx;
   }

   public void setGlobalTransaction(GlobalTransaction gtx) {
      globalTx = gtx;
   }

   public Collection<Object> getKeys() {
      return keys;
   }

   public void replaceKey(Object oldKey, Object replacement) {
      int i = keys.indexOf(oldKey);
      if (i >= 0) {
         keys.set(i, replacement);
      }
   }

   public void replaceKeys(Map<Object, Object> replacements) {
      for (int i = 0; i < keys.size(); i++) {
         Object replacement = replacements.get(keys.get(i));
         if (replacement != null) {
            keys.set(i, replacement);
         }
      }
   }

   public boolean multipleKeys() {
      return keys.size() > 1;
   }

   public Object getSingleKey() {
      if (keys.size() == 0)
         return null;

      return keys.get(0);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitLockControlCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public Object perform(InvocationContext ignored) throws Throwable {
      if (ignored != null)
         throw new IllegalStateException("Expected null context!");

      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

      if (transaction == null) {
         if (unlock) {
            log.tracef("Unlock for non-existant transaction %s.  Not doing anything.", globalTx);
            return null;
         }
         //create a remote tx without any modifications (we do not know modifications ahead of time)
         transaction = txTable.getOrCreateRemoteTransaction(globalTx, null);
      }
      RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext(transaction, getOrigin());
      return invoker.invoke(ctxt, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, unlock, keys, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      // TODO: Check duplicated in all commands? A better solution is needed.
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unsupported command id:" + commandId);
      int i = 0;
      globalTx = (GlobalTransaction) args[i++];
      unlock = (Boolean) args[i++];
      keys = (List<Object>) args[i++];
      flags = (Set<Flag>) args[i];
   }

   public boolean isUnlock() {
      return unlock;
   }

   public void setUnlock(boolean unlock) {
      this.unlock = unlock;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      LockControlCommand that = (LockControlCommand) o;

      if (unlock != that.unlock) return false;
      if (flags == null)
         return that.flags == null;
      else
         if (!flags.equals(that.flags)) return false;
      if (!keys.equals(that.keys)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + keys.hashCode();
      result = 31 * result + (unlock ? 1 : 0);
      if (flags != null)
         result = 31 * result + flags.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("LockControlCommand{cache=").append(cacheName)
         .append(", keys=").append(keys)
         .append(", flags=").append(flags)
         .append(", unlock=").append(unlock)
         .append("}")
         .toString();
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
   public boolean hasFlag(Flag flag) {
      return flags != null && flags.contains(flag);
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
   public Metadata getMetadata() {
      return null;
   }

}
