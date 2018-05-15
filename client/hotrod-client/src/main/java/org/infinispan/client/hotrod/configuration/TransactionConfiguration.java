package org.infinispan.client.hotrod.configuration;

import java.util.Properties;

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

   TransactionConfiguration(TransactionMode transactionMode,
         TransactionManagerLookup transactionManagerLookup) {
      this.transactionMode = transactionMode;
      this.transactionManagerLookup = transactionManagerLookup;
   }

   public TransactionMode transactionMode() {
      return transactionMode;
   }

   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup;
   }

   @Override
   public String toString() {
      return "TransactionConfiguration{" +
            "transactionMode=" + transactionMode +
            ", transactionManagerLookup=" + transactionManagerLookup +
            '}';
   }

   void toProperties(Properties properties) {
      properties.setProperty(ConfigurationProperties.TRANSACTION_MODE, transactionMode.name());
      properties.setProperty(ConfigurationProperties.TRANSACTION_MANAGER_LOOKUP, transactionManagerLookup.getClass().getName());
   }
}
