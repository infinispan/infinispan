package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSACTION_MANAGER_LOOKUP;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSACTION_MODE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSACTION_TIMEOUT;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;
import static org.infinispan.commons.util.Util.getInstance;
import static org.infinispan.commons.util.Util.loadClass;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configures a transactional {@link RemoteCache}.
 *
 * @author Pedro Ruivo
 * @since 9.3
 * @deprecated since 12.0. To be removed in Infinispan 14.
 */
@Deprecated
public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      Builder<TransactionConfiguration> {

   public static final long DEFAULT_TIMEOUT = 60000;

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
    * @deprecated since 12.0. To be removed in Infinispan 14. Use {@link RemoteCacheConfigurationBuilder#transactionManagerLookup(TransactionManagerLookup)}
    */
   @Deprecated
   public TransactionConfigurationBuilder transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transactionManagerLookup = transactionManagerLookup;
      return this;
   }

   /**
    * The {@link TransactionMode} in which a {@link RemoteCache} will be enlisted.
    * @deprecated since 12.0. To be removed in Infinispan 14. Use {@link RemoteCacheConfigurationBuilder#transactionMode(TransactionMode)}
    */
   @Deprecated
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
    * @deprecated since 12.0. To be removed in Infinispan 14. Use {@link ConfigurationBuilder#transactionTimeout(long, TimeUnit)}
    */
   @Deprecated
   public TransactionConfigurationBuilder timeout(long timeout, TimeUnit timeUnit) {
      setTimeoutMillis(timeUnit.toMillis(timeout));
      return this;
   }

   @Override
   public void validate() {
      if (transactionMode == null) {
         throw HOTROD.invalidTransactionMode();
      }
      if (transactionManagerLookup == null) {
         throw HOTROD.invalidTransactionManagerLookup();
      }
      if (timeout <= 0) {
         throw HOTROD.invalidTransactionTimeout();
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
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      transactionMode(typed.getEnumProperty(TRANSACTION_MODE, TransactionMode.class, transactionMode, true));
      transactionManagerLookup(tlmFromString(typed.getProperty(TRANSACTION_MANAGER_LOOKUP, tlmClass(), true)));
      setTimeoutMillis(typed.getLongProperty(TRANSACTION_TIMEOUT, timeout, true));
   }

   private TransactionManagerLookup tlmFromString(String lookupClass) {
      return lookupClass == null || tlmClass().equals(lookupClass) ?
            transactionManagerLookup :
            getInstance(loadClass(lookupClass, builder.classLoader()));
   }

   private String tlmClass() {
      return transactionManagerLookup.getClass().getName();
   }

   private void setTimeoutMillis(long timeout) {
      this.timeout = timeout;
   }
}
