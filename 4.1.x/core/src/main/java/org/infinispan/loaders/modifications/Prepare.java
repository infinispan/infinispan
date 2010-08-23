/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.loaders.modifications;

import java.util.List;

import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Prepare.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class Prepare implements Modification {
   final List<? extends Modification> list;
   final GlobalTransaction tx;
   final boolean isOnePhase;

   public Prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) {
      this.list = list;
      this.tx = tx;
      this.isOnePhase = isOnePhase;
   }

   @Override
   public Type getType() {
      return Type.PREPARE;
   }

   public List<? extends Modification> getList() {
      return list;
   }

   public GlobalTransaction getTx() {
      return tx;
   }

   public boolean isOnePhase() {
      return isOnePhase;
   }

   @Override
   public boolean equals(Object obj) {
      if (obj == this)
         return true;
      if (!(obj instanceof Prepare))
         return false;
      Prepare other = (Prepare) obj;
      return list.equals(other.list) 
         && tx.equals(other.tx) 
         && isOnePhase == other.isOnePhase;
   }

   @Override
   public int hashCode() {
      int result = 17;
      result = 31 * result + list.hashCode();
      result = 31 * result + tx.hashCode();
      result = 31 * result + (isOnePhase ? 1 : 0);
      return result;
   }

}
