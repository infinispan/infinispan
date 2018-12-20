package org.infinispan.client.hotrod.configuration;

import static org.infinispan.commons.util.Util.getInstance;
import static org.infinispan.commons.util.Util.loadClass;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

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

   public static final long DEFAULT_TIMEOUT = 60000;

   private static final Log log = LogFactory.getLog(TransactionConfigurationBuilder.class, Log.class);
   private TransactionMode transactionMode = TransactionMode.NONE;
   private TransactionManagerLookup transactionManagerLookup = defaultTransactionManagerLookup();
   private long timeout = DEFAULT_TIMEOUT;

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

   /**
    * Sets the transaction's timeout.
    * <p>
    * This timeout is used by the server to rollback unrecoverable transaction when they are idle for this amount of
    * time.
    * <p>
    * An unrecoverable transaction are transaction enlisted as {@link Synchronization} ({@link TransactionMode#NON_XA})
    * or {@link XAResource} without recovery enabled ({@link TransactionMode#NON_DURABLE_XA}).
    * <p>
    * For {@link XAResource}, this value is overwritten by {@link XAResource#setTransactionTimeout(int)}.
    * <p>
    * It defaults to 1 minute.
    */
   public TransactionConfigurationBuilder timeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeUnit.toMillis(timeout);
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
      if (timeout <= 0) {
         throw log.invalidTransactionTimeout();
      }
   }

   @Override
   public TransactionConfiguration create() {
      return new TransactionConfiguration(transactionMode, transactionManagerLookup, timeout);
   }

   @Override
   public Builder<?> read(TransactionConfiguration template) {
      this.transactionManagerLookup = template.transactionManagerLookup();
      this.transactionMode = template.transactionMode();
      this.timeout = template.timeout();
      return this;
   }

   void withTransactionProperties(Properties properties) {
      ConfigurationProperties cp = new ConfigurationProperties(properties);
      this.transactionMode = cp.getTransactionMode();
      String managerLookupClass = cp.getTransactionManagerLookup();
      if (managerLookupClass != null && !transactionManagerLookup.getClass().getName().equals(managerLookupClass)) {
         this.transactionManagerLookup = getInstance(loadClass(managerLookupClass, builder.classLoader()));
      }
      this.timeout = cp.getTransactionTimeout();
   }
}
