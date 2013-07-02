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
