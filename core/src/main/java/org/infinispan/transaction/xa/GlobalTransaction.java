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
package org.infinispan.transaction.xa;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;


/**
 * Uniquely identifies a transaction that spans all JVMs in a cluster. This is used when replicating all modifications
 * in a transaction; the PREPARE and COMMIT (or ROLLBACK) messages have to have a unique identifier to associate the
 * changes with<br>. GlobalTransaction should be instantiated thorough {@link TransactionFactory} class,
 * as their type depends on the runtime configuration.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 12, 2003
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class GlobalTransaction implements Cloneable {

   private static final long serialVersionUID = 8011434781266976149L;

   private static AtomicLong sid = new AtomicLong(0);

   long id = -1;

   protected transient Address addr = null;
   private transient int hash_code = -1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here
   private transient boolean remote = false;

   /**
    * empty ctor used by externalization.
    */
   protected GlobalTransaction() {
   }

   protected GlobalTransaction(Address addr, boolean remote) {
      this.id = sid.incrementAndGet();
      this.addr = addr;
      this.remote = remote;
   }

   public Address getAddress() {
      return addr;
   }

   public long getId() {
      return id;
   }

   public boolean isRemote() {
      return remote;
   }

   public void setRemote(boolean remote) {
      this.remote = remote;
   }


   @Override
   public int hashCode() {
      if (hash_code == -1) {
         hash_code = (addr != null ? addr.hashCode() : 0) + (int) id;
      }
      return hash_code;
   }

   @Override
   public boolean equals(Object other) {
      if (this == other)
         return true;
      if (!(other instanceof GlobalTransaction))
         return false;

      GlobalTransaction otherGtx = (GlobalTransaction) other;
      boolean aeq = (addr == null) ? (otherGtx.addr == null) : addr.equals(otherGtx.addr);
      return aeq && (id == otherGtx.id);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("GlobalTransaction:<").append(addr).append(">:").append(id).append(isRemote() ? ":remote" : ":local");
      return sb.toString();
   }

   public void setId(long id) {
      this.id = id;
   }

   public void setAddress(Address address) {
      this.addr = address;
   }

   @Override
   public Object clone() {
      try {
         return super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!");
      }
   }

   protected abstract static class AbstractGlobalTxExternalizer<T extends GlobalTransaction> extends AbstractExternalizer<T> {
      @Override
      public void writeObject(ObjectOutput output, T gtx) throws IOException {
         output.writeLong(gtx.id);
         output.writeObject(gtx.addr);
      }

      /**
       * Factory method for GlobalTransactions
       * @return a newly constructed instance of GlobalTransaction or one of its subclasses
       **/
      protected abstract T createGlobalTransaction();

      @Override
      public T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         T gtx = createGlobalTransaction();
         gtx.id = input.readLong();
         gtx.addr = (Address) input.readObject();
         return gtx;
      }
   }

   public static class Externalizer extends AbstractGlobalTxExternalizer<GlobalTransaction> {
      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends GlobalTransaction>> getTypeClasses() {
         return Util.<Class<? extends GlobalTransaction>>asSet(GlobalTransaction.class);
      }

      @Override
      public Integer getId() {
         return Ids.GLOBAL_TRANSACTION;
      }

      @Override
      protected GlobalTransaction createGlobalTransaction() {
         return TransactionFactory.TxFactoryEnum.NODLD_NORECOVERY_XA.newGlobalTransaction();
      }
   }
}