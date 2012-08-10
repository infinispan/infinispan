/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * A representation of a transaction that is suitable for transferring between a StateProvider and a StateConsumer
 * running on different members of the same cache.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class TransactionInfo {

   private final GlobalTransaction globalTransaction;

   private final WriteCommand[] modifications;

   private final Set<Object> lockedKeys;

   public TransactionInfo(GlobalTransaction globalTransaction, WriteCommand[] modifications, Set<Object> lockedKeys) {
      this.globalTransaction = globalTransaction;
      this.modifications = modifications;
      this.lockedKeys = lockedKeys;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public WriteCommand[] getModifications() {
      return modifications;
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys;
   }

   @Override
   public String toString() {
      return "TransactionInfo{" +
            "globalTransaction=" + globalTransaction +
            ", modifications=" + Arrays.asList(modifications) +
            ", lockedKeys=" + lockedKeys +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<TransactionInfo> {

      @Override
      public Integer getId() {
         return Ids.TRANSACTION_INFO;
      }

      @Override
      public Set<Class<? extends TransactionInfo>> getTypeClasses() {
         return Collections.<Class<? extends TransactionInfo>>singleton(TransactionInfo.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TransactionInfo object) throws IOException {
         output.writeObject(object.globalTransaction);
         output.writeObject(object.modifications);
         output.writeObject(object.lockedKeys);
      }

      @Override
      @SuppressWarnings("unchecked")
      public TransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         GlobalTransaction globalTransaction = (GlobalTransaction) input.readObject();
         WriteCommand[] modifications = (WriteCommand[]) input.readObject();
         Set<Object> lockedKeys = (Set<Object>) input.readObject();
         return new TransactionInfo(globalTransaction, modifications, lockedKeys);
      }
   }
}
