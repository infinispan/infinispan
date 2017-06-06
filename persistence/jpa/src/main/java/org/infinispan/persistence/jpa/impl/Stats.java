package org.infinispan.persistence.jpa.impl;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Stats {

   private class Operation {
      final LongAdder count = new LongAdder();
      final LongAdder sum = new LongAdder();

      void add(long duration) {
         count.increment();
         sum.add(duration);
      }

      @Override
      public String toString() {
         long count = this.count.sum();
         long sum = this.sum.sum();
         return String.format("[count=%d, avg=%.2f ms]", count, count == 0 ? Double.NaN : sum / (1000000.0 * count));
      }
   }

   private final Operation entityFind = new Operation();
   private final Operation entityMerge = new Operation();
   private final Operation entityRemove = new Operation();
   private final Operation metadataFind = new Operation();
   private final Operation metadataMerge = new Operation();
   private final Operation metadataRemove = new Operation();
   private final Operation txReadCommitted = new Operation();
   private final Operation txWriteCommitted = new Operation();
   private final Operation txRemoveCommitted = new Operation();
   private final Operation txBatchWriteCommitted = new Operation();
   private final Operation txBatchRemoveCommitted = new Operation();
   private final Operation txReadFailed = new Operation();
   private final Operation txWriteFailed = new Operation();
   private final Operation txRemoveFailed = new Operation();
   private final Operation txBatchWriteFailed = new Operation();
   private final Operation txBatchRemoveFailed = new Operation();


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

   public void addBatchWriteTxCommitted(long duration) {
      txBatchWriteCommitted.add(duration);
   }

   public void addBatchWriteTxFailed(long duration) {
      txBatchWriteFailed.add(duration);
   }

   public void addBatchRemoveTxCommitted(long duration) {
      txBatchRemoveCommitted.add(duration);
   }

   public void addBatchRemoveTxFailed(long duration) {
      txBatchRemoveFailed.add(duration);
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
            "\ntxWriteCommitted=" + txWriteCommitted +
            "\ntxRemoveCommitted=" + txRemoveCommitted +
            "\ntxBatchWriteCommitted=" + txBatchWriteCommitted +
            "\ntxBatchRemoveCommitted=" + txBatchRemoveCommitted +
            "\ntxReadFailed=" + txReadFailed +
            "\ntxWriteFailed=" + txWriteFailed +
            "\ntxRemoveFailed=" + txRemoveFailed +
            "\ntxBatchWriteFailed=" + txBatchWriteFailed +
            "\ntxBatchRemoveFailed=" + txBatchRemoveFailed +
            '}';
   }
}
