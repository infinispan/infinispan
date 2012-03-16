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

package org.infinispan.commands.remote.recovery;

import org.infinispan.context.InvocationContext;
import javax.transaction.xa.Xid;

/**
 * Command used by the recovery tooling for forcing transaction completion .
 *
 * @author Mircea Markus
 * @since 5.0
 */
public class CompleteTransactionCommand extends RecoveryCommand {

   public static final byte COMMAND_ID = 24;

   /**
    * The tx which we want to complete.
    */
   private Xid xid;

   /**
    * if true the transaction is committed, otherwise it is rolled back.
    */
   private boolean commit;

   private CompleteTransactionCommand() {
      super(null); // For command id uniqueness test
   }

   public CompleteTransactionCommand(String cacheName) {
      super(cacheName);
   }

   public CompleteTransactionCommand(String cacheName, Xid xid, boolean commit) {
      super(cacheName);
      this.xid = xid;
      this.commit = commit;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return recoveryManager.forceTransactionCompletion(xid, commit);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{xid, commit};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Unexpected command id: " + commandId + ". Expected " + COMMAND_ID);
      }
      int i = 0;
      xid = (Xid) parameters[i++];
      commit = (Boolean) parameters[i++];
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", commit=" + commit +
            ", cacheName=" + cacheName +
            "} ";
   }
}
