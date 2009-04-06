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
package org.horizon.commands.tx;

import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.Visitor;
import org.horizon.commands.write.WriteCommand;
import org.horizon.context.InvocationContext;
import org.horizon.remoting.transport.Address;
import org.horizon.transaction.GlobalTransaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: MANIK: Document this
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand {
   public static final byte METHOD_ID = 10;

   protected WriteCommand[] modifications;
   protected Address localAddress;
   protected boolean onePhaseCommit;

   public PrepareCommand(GlobalTransaction gtx, Address localAddress, boolean onePhaseCommit, WriteCommand... modifications) {
      this.gtx = gtx;
      this.modifications = modifications;
      this.localAddress = localAddress;
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(GlobalTransaction gtx, List<WriteCommand> commands, Address localAddress, boolean onePhaseCommit) {
      this.gtx = gtx;
      this.modifications = commands == null || commands.size() == 0 ? null : commands.toArray(new WriteCommand[commands.size()]);
      this.localAddress = localAddress;
      this.onePhaseCommit = onePhaseCommit;
   }

   public void removeModifications(Collection<WriteCommand> modificationsToRemove) {
      if (modifications != null && modificationsToRemove != null && modificationsToRemove.size() > 0) {
         // defensive copy
         Set<WriteCommand> toRemove = new HashSet<WriteCommand>(modificationsToRemove);
         WriteCommand[] newMods = new WriteCommand[modifications.length - modificationsToRemove.size()];
         int i = 0;
         for (WriteCommand c : modifications) {
            if (toRemove.contains(c)) {
               toRemove.remove(c);
            } else {
               newMods[i++] = c;
            }
         }
         modifications = newMods;
      }
   }

   public PrepareCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareCommand(ctx, this);
   }

   public WriteCommand[] getModifications() {
      return modifications;
   }

   public Address getLocalAddress() {
      return localAddress;
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
      return METHOD_ID;
   }

   @Override
   public Object[] getParameters() {
      int numMods = modifications == null ? 0 : modifications.length;
      Object[] retval = new Object[numMods + 4];
      retval[0] = gtx;
      retval[1] = localAddress;
      retval[2] = onePhaseCommit;
      retval[3] = numMods;
      if (numMods > 0) System.arraycopy(modifications, 0, retval, 4, numMods);
      return retval;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      gtx = (GlobalTransaction) args[0];
      localAddress = (Address) args[1];
      onePhaseCommit = (Boolean) args[2];
      int numMods = (Integer) args[3];
      if (numMods > 0) {
         modifications = new WriteCommand[numMods];
         System.arraycopy(args, 4, modifications, 0, numMods);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      PrepareCommand that = (PrepareCommand) o;

      if (onePhaseCommit != that.onePhaseCommit) return false;
      if (localAddress != null ? !localAddress.equals(that.localAddress) : that.localAddress != null) return false;
      if (modifications != null ? !Arrays.equals(modifications, that.modifications) : that.modifications != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (modifications != null ? modifications.hashCode() : 0);
      result = 31 * result + (localAddress != null ? localAddress.hashCode() : 0);
      result = 31 * result + (onePhaseCommit ? 1 : 0);
      return result;
   }

   public PrepareCommand copy() {
      PrepareCommand copy = new PrepareCommand();
      copy.gtx = gtx;
      copy.localAddress = localAddress;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      return copy;
   }

   @Override
   public String toString() {
      return "PrepareCommand{" +
            "globalTransaction=" + gtx +
            ", modifications=" + Arrays.toString(modifications) +
            ", localAddress=" + localAddress +
            ", onePhaseCommit=" + onePhaseCommit +
            '}';
   }

   public boolean containsModificationType(Class<? extends ReplicableCommand> replicableCommandClass) {
      for (WriteCommand mod : getModifications()) {
         if (mod.getClass().equals(replicableCommandClass)) {
            return true;
         }
      }
      return false;
   }
}
