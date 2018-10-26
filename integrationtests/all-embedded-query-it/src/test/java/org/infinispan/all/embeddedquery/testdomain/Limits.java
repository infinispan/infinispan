package org.infinispan.all.embeddedquery.testdomain;

/**
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public interface Limits {

   Double getMaxDailyLimit();

   void setMaxDailyLimit(Double maxDailyLimit);

   Double getMaxTransactionLimit();

   void setMaxTransactionLimit(Double maxTransactionLimit);

   String[] getPayees();

   void setPayees(String[] payees);
}
