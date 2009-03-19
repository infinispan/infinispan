/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.transaction;

import org.horizon.remoting.transport.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Uniquely identifies a transaction that spans all JVMs in a cluster. This is used when replicating all modifications
 * in a transaction; the PREPARE and COMMIT (or ROLLBACK) messages have to have a unique identifier to associate the
 * changes with<br>
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a> Apr 12, 2003
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 1.0
 */
public class GlobalTransaction implements Externalizable {

   private static final long serialVersionUID = 8011434781266976149L;

   private static AtomicLong sid = new AtomicLong(0);

   private Address addr = null;
   private long id = -1;
   private transient boolean remote = false;

   // cache the hashcode
   private transient int hash_code = -1;  // in the worst case, hashCode() returns 0, then increases, so we're safe here

   /**
    * empty ctor used by externalization
    */
   public GlobalTransaction() {
   }

   private GlobalTransaction(Address addr) {
      this.addr = addr;
      id = sid.getAndIncrement();
   }

   public static GlobalTransaction create(Address addr) {
      return new GlobalTransaction(addr);
   }

   public Object getAddress() {
      return addr;
   }

   public void setAddress(Address address) {
      addr = address;
   }

   public long getId() {
      return id;
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
      sb.append("GlobalTransaction:<").append(addr).append(">:").append(id);
      return sb.toString();
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(addr);
      out.writeLong(id);
      // out.writeInt(hash_code);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      addr = (Address) in.readObject();
      id = in.readLong();
      hash_code = -1;
   }

   /**
    * @return Returns the remote.
    */
   public boolean isRemote() {
      return remote;
   }

   /**
    * @param remote The remote to set.
    */
   public void setRemote(boolean remote) {
      this.remote = remote;
   }


   public void setId(long id) {
      this.id = id;
   }
}