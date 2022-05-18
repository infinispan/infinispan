package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.Limits;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class LimitsHS implements Limits, Serializable {

   private Double maxDailyLimit;

   private Double maxTransactionLimit;

   private String[] payees = new String[0];

   @Override
   @ProtoField(number = 1)
   @Basic(projectable = true)
   public Double getMaxDailyLimit() {
      return maxDailyLimit;
   }

   @Override
   public void setMaxDailyLimit(Double maxDailyLimit) {
      this.maxDailyLimit = maxDailyLimit;
   }

   @Override
   @ProtoField(number = 2)
   @Basic(projectable = true)
   public Double getMaxTransactionLimit() {
      return maxTransactionLimit;
   }

   @Override
   public void setMaxTransactionLimit(Double maxTransactionLimit) {
      this.maxTransactionLimit = maxTransactionLimit;
   }

   @Override
   @ProtoField(number = 3)
   @Basic(projectable = true)
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
      LimitsHS limits = (LimitsHS) o;
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
      return "LimitsHS{" +
            "maxDailyLimit=" + maxDailyLimit +
            ", maxTransactionLimit=" + maxTransactionLimit +
            ", payees=" + Arrays.toString(payees) +
            '}';
   }
}
