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
 * @deprecated since 12.0. To be removed in Infinispan 14
 */
@Deprecated
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

   @Deprecated
   public TransactionMode transactionMode() {
      return transactionMode;
   }

   @Deprecated
   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup;
   }

   /**
    * @see TransactionConfigurationBuilder#timeout(long, TimeUnit)
    */
   @Deprecated
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
