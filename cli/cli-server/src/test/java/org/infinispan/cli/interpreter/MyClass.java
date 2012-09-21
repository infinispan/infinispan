/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;

public class MyClass {
   int i;
   String s;
   boolean b;
   MyClass x;
   double d;

   public int getI() {
      return i;
   }

   public void setI(int i) {
      this.i = i;
   }

   public String getS() {
      return s;
   }

   public void setS(String s) {
      this.s = s;
   }

   public boolean isB() {
      return b;
   }

   public void setB(boolean b) {
      this.b = b;
   }

   public MyClass getX() {
      return x;
   }

   public void setX(MyClass x) {
      this.x = x;
   }

   @Override
   public String toString() {
      return "MyClass [i=" + i + ", s=" + s + ", b=" + b + ", x=" + x + ", d=" + d + "]";
   }
}
