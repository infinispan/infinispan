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
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;
import java.util.List;

/**
 * Rpc to obtain all in-doubt prepared transactions stored on remote nodes.
 * A transaction is in doubt if it is prepared and the node where it started has crashed.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class GetInDoubtTransactionsCommand extends RecoveryCommand {

   private static final Log log = LogFactory.getLog(GetInDoubtTransactionsCommand.class);

   public static final int COMMAND_ID = 21;

   private GetInDoubtTransactionsCommand() {
      super(null); // For command id uniqueness test
   }

   public GetInDoubtTransactionsCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public List<Xid> perform(InvocationContext ctx) throws Throwable {
      List<Xid> localInDoubtTransactions = recoveryManager.getInDoubtTransactions();
      if (log.isTraceEnabled()) log.tracef("Returning result %s", localInDoubtTransactions);
      return localInDoubtTransactions;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Expected " + COMMAND_ID + "and received " + commandId);
      // No parameters
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }
}
