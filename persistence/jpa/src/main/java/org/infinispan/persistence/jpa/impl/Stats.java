package org.infinispan.persistence.jpa.impl;

import org.infinispan.commons.util.concurrent.jdk8backported.LongAdder;

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
   private final Operation txReadFailed = new Operation();
   private final Operation txWriteFailed = new Operation();
   private final Operation txRemoveFailed = new Operation();


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
