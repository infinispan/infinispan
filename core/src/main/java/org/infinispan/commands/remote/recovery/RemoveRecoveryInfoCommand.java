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
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RemoveRecoveryInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = 22;

   private Xid xid;
   private long internalId;

   private RemoveRecoveryInfoCommand() {
      super(null); // For command id uniqueness test
   }

   public RemoveRecoveryInfoCommand(Xid xid, String cacheName) {
      super(cacheName);
      this.xid = xid;
   }

   public RemoveRecoveryInfoCommand(long internalId, String cacheName) {
      super(cacheName);
      this.internalId = internalId;
   }

   public RemoveRecoveryInfoCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (xid != null) {
         recoveryManager.removeRecoveryInformation(xid);
      } else {
         recoveryManager.removeRecoveryInformation(internalId);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{xid != null ? xid : internalId};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + RemoveRecoveryInfoCommand.COMMAND_ID);
      }
      if (parameters[0] instanceof Xid) {
         xid = (Xid) parameters[0];
      } else {
         internalId = (Long) parameters[0];
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", internalId=" + internalId +
            ", cacheName=" + cacheName + "} ";
   }
}
