package org.infinispan.client.hotrod.configuration;

import static org.infinispan.commons.util.Util.getInstance;
import static org.infinispan.commons.util.Util.loadClass;

import java.util.Properties;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * Configures a transactional {@link RemoteCache}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      Builder<TransactionConfiguration> {

   private static final Log log = LogFactory.getLog(TransactionConfigurationBuilder.class, Log.class);
   private TransactionMode transactionMode = TransactionMode.NONE;
   private TransactionManagerLookup transactionManagerLookup = defaultTransactionManagerLookup();
   TransactionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public static TransactionManagerLookup defaultTransactionManagerLookup() {
      return GenericTransactionManagerLookup.getInstance();
   }

   /**
    * The {@link TransactionManagerLookup} to lookup for the {@link TransactionManager} to interact with.
    */
   public TransactionConfigurationBuilder transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transactionManagerLookup = transactionManagerLookup;
      return this;
   }

   /**
    * The {@link TransactionMode} in which a {@link RemoteCache} will be enlisted.
    */
   public TransactionConfigurationBuilder transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   @Override
   public void validate() {
      if (transactionMode == null) {
         throw log.invalidTransactionMode();
      }
      if (transactionManagerLookup == null) {
         throw log.invalidTransactionManagerLookup();
      }
   }

   @Override
   public TransactionConfiguration create() {
      return new TransactionConfiguration(transactionMode, transactionManagerLookup);
   }

   @Override
   public Builder<?> read(TransactionConfiguration template) {
      this.transactionManagerLookup = template.transactionManagerLookup();
      this.transactionMode = template.transactionMode();
      return this;
   }

   void withTransactionProperties(Properties properties) {
      ConfigurationProperties cp = new ConfigurationProperties(properties);
      this.transactionMode = cp.getTransactionMode();
      this.transactionManagerLookup = getInstance(loadClass(cp.getTransactionManagerLookup(), builder.classLoader()));
   }
}
