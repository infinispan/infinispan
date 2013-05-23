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
package org.infinispan.test.data;

import org.infinispan.util.concurrent.ReclosableLatch;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.TimeUnit;

public class Key implements Externalizable {

   private String value;
   private final ReclosableLatch latch = new ReclosableLatch(false);
   private final boolean lockable;
   private static final long serialVersionUID = 4745232904453872125L;

   public Key() {
      this.lockable = false;
   }

   public Key(String value, boolean lockable) {
      this.value = value;
      this.lockable = lockable;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Key k1 = (Key) o;

      if (value != null ? !value.equals(k1.value) : k1.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(value);
      if (lockable) {
         try {
            if (!latch.await(1, TimeUnit.MINUTES)) throw new RuntimeException("Cannot serialize!!");
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         latch.close();
      }
   }

   public void allowSerialization() {
      if (lockable) latch.open();
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      value = (String) in.readObject();
   }
}
