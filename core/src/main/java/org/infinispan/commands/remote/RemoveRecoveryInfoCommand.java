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
package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;

import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.List;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RemoveRecoveryInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = Ids.REMOVE_RECOVERY_INFO_TX_COMMAND;

   private volatile List<Xid> xids;

   public RemoveRecoveryInfoCommand(List<Xid> xids, String cacheName) {
      this.xids = xids;
      this.cacheName = cacheName;
   }

   public RemoveRecoveryInfoCommand() {
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      recoveryManager.removeLocalRecoveryInformation(xids);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] result = new Object[2];
      result[0] = xids;
      result[1] = cacheName;
      return result;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + Ids.REMOVE_RECOVERY_INFO_TX_COMMAND);
      }
      xids = (List<Xid>) parameters[0];
      cacheName = (String) parameters[1];
   }

   @Override
   public String toString() {
      return "RemoveRecoveryInfoCommand {" +
            "xids=" + (xids == null ? null : Arrays.asList(xids)) +
            "} " + super.toString();
   }
}
