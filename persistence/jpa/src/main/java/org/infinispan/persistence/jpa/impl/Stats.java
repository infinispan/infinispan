package org.infinispan.persistence.jpa.impl;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Stats {

   private class Operation {
      AtomicLong count = new AtomicLong();
      AtomicLong sum = new AtomicLong();

      void add(long duration) {
         count.incrementAndGet();
         sum.addAndGet(duration);
      }

      @Override
      public String toString() {
         long count = this.count.get();
         long sum = this.sum.get();
         return String.format("[count=%d, avg=%.2f ms]", count, count == 0 ? Double.NaN : sum / (1000000.0 * count));
      }
   }

   private Operation entityFind = new Operation();
   private Operation entityMerge = new Operation();
   private Operation entityRemove = new Operation();
   private Operation metadataFind = new Operation();
   private Operation metadataMerge = new Operation();
   private Operation metadataRemove = new Operation();
   private Operation txReadCommitted = new Operation();
   private Operation txWriteCommitted = new Operation();
   private Operation txRemoveCommitted = new Operation();
   private Operation txReadFailed = new Operation();
   private Operation txWriteFailed = new Operation();
   private Operation txRemoveFailed = new Operation();



   public void addEntityMerge(long duration) {
      entityMerge.add(duration);
   }

   public void addMetadataMerge(long duration) {
      metadataMerge.add(duration);
   }

   public void addWriteTxCommited(long duration) {
      txWriteCommitted.add(duration);
   }

   public void addWriteTxFailed(long duration) {
      txWriteFailed.add(duration);
   }

   public void addEntityFind(long duration) {
      entityFind.add(duration);
   }

   public void addMetadataFind(long duration) {
      metadataFind.add(duration);
   }

   public void addReadTxCommitted(long duration) {
      txReadCommitted.add(duration);
   }

   public void addReadTxFailed(long duration) {
      txReadFailed.add(duration);
   }

   public void addEntityRemove(long duration) {
      entityRemove.add(duration);
   }

   public void addMetadataRemove(long duration) {
      metadataRemove.add(duration);
   }

   public void addRemoveTxCommitted(long duration) {
      txRemoveCommitted.add(duration);
   }

   public void addRemoveTxFailed(long duration) {
      txRemoveFailed.add(duration);
   }

   @Override
   public String toString() {
      return "Stats{" +
            "\nentityFind=" + entityFind +
            "\nentityMerge=" + entityMerge +
            "\nentityRemove=" + entityRemove +
            "\nmetadataFind=" + metadataFind +
            "\nmetadataMerge=" + metadataMerge +
            "\nmetadataRemove=" + metadataRemove +
            "\ntxReadCommitted=" + txReadCommitted +
            "\ntxReadFailed=" + txReadFailed +
            "\ntxWriteCommitted=" + txWriteCommitted +
            "\ntxWriteFailed=" + txWriteFailed +
            "\ntxRemoveCommitted=" + txRemoveCommitted +
            "\ntxRemoveFailed=" + txRemoveFailed +
            '}';
   }
}
