package org.infinispan.all.embeddedquery.testdomain.hsearch;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.impl.BuiltinArrayBridge;
import org.infinispan.all.embeddedquery.testdomain.Limits;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public class LimitsHS implements Limits, Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Double maxDailyLimit;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Double maxTransactionLimit;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @FieldBridge(impl = BuiltinArrayBridge.class)
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
