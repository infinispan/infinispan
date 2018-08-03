package org.infinispan.client.hotrod.configuration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * Configures a transactional {@link RemoteCache}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class TransactionConfiguration {

   private final TransactionMode transactionMode;
   private final TransactionManagerLookup transactionManagerLookup;
   private final long timeout;

   TransactionConfiguration(TransactionMode transactionMode,
         TransactionManagerLookup transactionManagerLookup, long timeout) {
      this.transactionMode = transactionMode;
      this.transactionManagerLookup = transactionManagerLookup;
      this.timeout = timeout;
   }

   public TransactionMode transactionMode() {
      return transactionMode;
   }

   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup;
   }

   /**
    * @see TransactionConfigurationBuilder#timeout(long, TimeUnit)
    */
   public long timeout() {
      return timeout;
   }

   @Override
   public String toString() {
      return "TransactionConfiguration{" +
             "transactionMode=" + transactionMode +
             ", transactionManagerLookup=" + transactionManagerLookup +
             ", timeout=" + timeout +
             '}';
   }

   void toProperties(Properties properties) {
      properties.setProperty(ConfigurationProperties.TRANSACTION_MODE, transactionMode.name());
      properties.setProperty(ConfigurationProperties.TRANSACTION_MANAGER_LOOKUP, transactionManagerLookup.getClass().getName());
      properties.setProperty(ConfigurationProperties.TRANSACTION_TIMEOUT, String.valueOf(timeout));
   }
}
