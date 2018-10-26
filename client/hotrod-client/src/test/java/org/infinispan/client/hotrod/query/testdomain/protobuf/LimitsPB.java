package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.util.Arrays;
import java.util.Objects;

import org.infinispan.query.dsl.embedded.testdomain.Limits;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class LimitsPB implements Limits {

   private Double maxDailyLimit;

   private Double maxTransactionLimit;

   private String[] payees = new String[0];

   @Override
   public Double getMaxDailyLimit() {
      return maxDailyLimit;
   }

   @Override
   public void setMaxDailyLimit(Double maxDailyLimit) {
      this.maxDailyLimit = maxDailyLimit;
   }

   @Override
   public Double getMaxTransactionLimit() {
      return maxTransactionLimit;
   }

   @Override
   public void setMaxTransactionLimit(Double maxTransactionLimit) {
      this.maxTransactionLimit = maxTransactionLimit;
   }

   @Override
   public String[] getPayees() {
      return payees;
   }

   @Override
   public void setPayees(String[] payees) {
      this.payees = payees;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LimitsPB limits = (LimitsPB) o;
      return Objects.equals(maxDailyLimit, limits.maxDailyLimit) &&
            Objects.equals(maxTransactionLimit, limits.maxTransactionLimit) &&
            Arrays.equals(payees, limits.payees);
   }

   @Override
   public int hashCode() {
      return Objects.hash(maxDailyLimit, maxTransactionLimit, Arrays.hashCode(payees));
   }

   @Override
   public String toString() {
      return "LimitsPB{" +
            "maxDailyLimit=" + maxDailyLimit +
            ", maxTransactionLimit=" + maxTransactionLimit +
            ", payees=" + Arrays.toString(payees) +
            '}';
   }
}
