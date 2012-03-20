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
package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Command corresponding to the 2nd phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommitCommand extends AbstractTransactionBoundaryCommand {
   public static final byte COMMAND_ID = 14;
   /**
    * This is sent back to callers if the global transaction is not reconised.  It can happen if a prepare is sent to one
    * set of nodes, which then fail, and a commit is sent to a "new" data owner which has not seen the prepare.
    *
    * Responding with this value instructs the caller to re-send the prepare.  See DistributionInterceptor.visitCommitCommand()
    * for details.
    */
   public static final byte RESEND_PREPARE = 1;

   private CommitCommand() {
      super(null); // For command id uniqueness test
   }

   public CommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName);
      this.globalTx = gtx;
   }

   public CommitCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   protected Object invalidRemoteTxReturnValue() {
      return RESEND_PREPARE;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitCommitCommand((TxInvocationContext) ctx, this);
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "CommitCommand {" + super.toString();
   }

   /**
    * choose the method to invoke depending if the total order protocol is be used or not
    *
    * @param ctx the context
    * @return the value to be returned to the invoked
    * @throws Throwable if something goes wrong
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (configuration.isTotalOrder()) {
         return super.performIgnoringUnexistingTransaction(ctx);
      } else {
         return super.perform(ctx);
      }
   }

}
