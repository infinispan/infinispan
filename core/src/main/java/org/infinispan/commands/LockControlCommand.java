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
package org.infinispan.commands;

import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.RemoteTransaction;

import java.util.Collection;

/**
 * LockControlCommand is a command that enables distributed locking across infinispan nodes.
 * <p> 
 * For more details refer to:
 * https://jira.jboss.org/jira/browse/ISPN-70
 * https://jira.jboss.org/jira/browse/ISPN-48
 * 
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @param
 * @since 4.0
 */
public class LockControlCommand extends AbstractTransactionBoundaryCommand {
   public static final int COMMAND_ID = 3;
   private Collection keys;
   private boolean implicit = false;

   public LockControlCommand() {
   }

   public LockControlCommand(Collection keys, String cacheName) {
      this(keys,cacheName,false);
   }
   
   public LockControlCommand(Collection keys, String cacheName, boolean implicit) {
      this.cacheName = cacheName;
      this.keys = keys;
      this.implicit = implicit;
   }

   public void attachGlobalTransaction(GlobalTransaction gtx) {
      globalTx = gtx;
   }

   public Collection getKeys() {
      return keys;
   }
   
   public boolean isImplicit() {
      return implicit;
   }
   
   public boolean isExplicit(){
      return !isImplicit();
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitLockControlCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public Object perform(InvocationContext ignored) throws Throwable {
      if (ignored != null)
         throw new IllegalStateException("Expected null context!");
      
      RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext();

      boolean remoteTxinitiated = txTable.getRemoteTransaction(globalTx) != null ? true : false;
      RemoteTransaction transaction =null;
      if (!remoteTxinitiated) {
         //create bogus modifications (we do not know modifications ahead of time)
         transaction = txTable.createRemoteTransaction(globalTx, new WriteCommand[]{});
         ctxt.setRemoteTransaction(transaction);   
      }
      return invoker.invoke(ctxt, this);
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{globalTx, cacheName, keys};
   }

   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unusupported command id:" + commandId);
      globalTx = (GlobalTransaction) args[0];
      cacheName = (String) args[1];
      keys = (Collection) args[2];
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      LockControlCommand that = (LockControlCommand) o;
      if (!super.equals(that))
         return false;
      return keys.equals(that.getKeys());
   }

   public int hashCode() {
      int result = super.hashCode();
      return 31 * result + (keys != null ? keys.hashCode() : 0);
   }

   @Override
   public String toString() {
      return "LockControlCommand{" +
            "gtx=" + globalTx +
            ", cacheName='" + cacheName +
            ", implicit='" + implicit +
            ", keys=" + keys + '}';
   }
}
