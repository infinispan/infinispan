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
package org.infinispan.query.test;

import org.infinispan.query.Transformable;

import java.io.Serializable;

@Transformable
public class CustomKey2 implements Serializable {
   int i, j, k;
   private static final long serialVersionUID = -8825579871900146417L;

   public CustomKey2(int i, int j, int k) {
      this.i = i;
      this.j = j;
      this.k = k;
   }

   public CustomKey2() {
   }

   public int getI() {
      return i;
   }

   public void setI(int i) {
      this.i = i;
   }

   public int getJ() {
      return j;
   }

   public void setJ(int j) {
      this.j = j;
   }

   public int getK() {
      return k;
   }

   public void setK(int k) {
      this.k = k;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomKey2 that = (CustomKey2) o;

      if (i != that.i) return false;
      if (j != that.j) return false;
      if (k != that.k) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = i;
      result = 31 * result + j;
      result = 31 * result + k;
      return result;
   }
}