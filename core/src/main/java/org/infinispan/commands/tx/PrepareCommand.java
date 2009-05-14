/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.commands.tx;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.List;

/**
 * // TODO: MANIK: Document this
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand {

   private static Log log = LogFactory.getLog(PrepareCommand.class);
   private boolean trace = log.isTraceEnabled();

   public static final byte COMMAND_ID = 12;

   protected WriteCommand[] modifications;
   protected boolean onePhaseCommit;
   protected CacheNotifier notifier;

   public void initialize(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public PrepareCommand(GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
      this.globalTx = gtx;
      this.modifications = modifications;
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(GlobalTransaction gtx, List<WriteCommand> commands, boolean onePhaseCommit) {
      this.globalTx = gtx;
      this.modifications = commands == null || commands.size() == 0 ? null : commands.toArray(new WriteCommand[commands.size()]);
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand() {
   }

   public final Object perform(InvocationContext ignored) throws Throwable {
      if (ignored != null) throw new IllegalStateException("Expected null context!");

      //1. first create a remote transaction
      RemoteTransaction remoteTransaction = txTable.createRemoteTransaction(globalTx, modifications);

      //2. then set it on the invocation context
      RemoteTxInvocationContext ctx = icc.createRemoteTxInvocationContext();
      ctx.setRemoteTransaction(remoteTransaction);

      if (trace) log.trace("Invoking remotly orginated prepare: " + this);
      notifier.notifyTransactionRegistered(ctx.getGlobalTransaction(), ctx);
      return invoker.invoke(ctx, this);
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareCommand((TxInvocationContext) ctx, this);
   }

   public WriteCommand[] getModifications() {
      return modifications;
   }

   public boolean isOnePhaseCommit() {
      return onePhaseCommit;
   }

   public boolean existModifications() {
      return modifications != null && modifications.length > 0;
   }

   public int getModificationsCount() {
      return modifications != null ? modifications.length : 0;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      int numMods = modifications == null ? 0 : modifications.length;
      Object[] retval = new Object[numMods + 4];
      retval[0] = globalTx;
      retval[1] = cacheName;
      retval[2] = onePhaseCommit;
      retval[3] = numMods;
      if (numMods > 0) System.arraycopy(modifications, 0, retval, 4, numMods);
      return retval;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      cacheName = (String) args[1];
      onePhaseCommit = (Boolean) args[2];
      int numMods = (Integer) args[3];
      if (numMods > 0) {
         modifications = new WriteCommand[numMods];
         System.arraycopy(args, 4, modifications, 0, numMods);
      }
   }

   public PrepareCommand copy() {
      PrepareCommand copy = new PrepareCommand();
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      return copy;
   }

   @Override
   public String toString() {
      return "PrepareCommand{" +
            "gtx=" + globalTx +
            ", modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            "} " + super.toString();
   }

   public boolean containsModificationType(Class<? extends ReplicableCommand> replicableCommandClass) {
      for (WriteCommand mod : getModifications()) {
         if (mod.getClass().equals(replicableCommandClass)) {
            return true;
         }
      }
      return false;
   }

   public boolean hasModifications() {
      return modifications != null && modifications.length > 0;
   }
}
