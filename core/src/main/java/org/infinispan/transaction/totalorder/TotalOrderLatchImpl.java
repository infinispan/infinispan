/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.transaction.totalorder;

import java.util.concurrent.CountDownLatch;

/**
 * Implementation of {@code TotalOrderLatch}
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderLatchImpl implements TotalOrderLatch {

   private final String name;
   private final CountDownLatch latch;

   public TotalOrderLatchImpl(String name) {
      if (name == null) {
         throw new NullPointerException("Name cannot be null");
      }
      this.name = name;
      this.latch = new CountDownLatch(1);
   }

   @Override
   public boolean isBlocked() {
      return latch.getCount() > 0;
   }

   @Override
   public void unBlock() {
      latch.countDown();
   }

   @Override
   public void awaitUntilUnBlock() throws InterruptedException {
      latch.await();
   }

   @Override
   public String toString() {
      return "TotalOrderLatchImpl{" +
            "latch=" + latch +
            ", name='" + name + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TotalOrderLatchImpl that = (TotalOrderLatchImpl) o;

      return name.equals(that.name);

   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }
}
