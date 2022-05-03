package org.infinispan.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.hotrod.transaction.lookup.GenericTransactionManagerLookup;

/**
 * @since 14.0
 **/
public class RemoteCacheConfiguration extends ConfigurationElement<RemoteCacheConfiguration> {
   public static final AttributeDefinition<String> CONFIGURATION = AttributeDefinition.builder("configuration", null, String.class).build();
   public static final AttributeDefinition<Boolean> FORCE_RETURN_VALUES = AttributeDefinition.builder("force-return-values", false, Boolean.class).build();
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   public static final AttributeDefinition<String> TEMPLATE_NAME = AttributeDefinition.builder("template-name", null, String.class).build();
   public static final AttributeDefinition<TransactionMode> TRANSACTION_MODE = AttributeDefinition.builder("transaction-mode", TransactionMode.NONE).build();
   public static final AttributeDefinition<TransactionManagerLookup> TRANSACTION_MANAGER = AttributeDefinition.builder("transaction-manager", GenericTransactionManagerLookup.getInstance(), TransactionManagerLookup.class).build();
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class).build();
   public static final AttributeDefinition<Class> MARSHALLER_CLASS = AttributeDefinition.builder("marshallerClass", null, Class.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteCacheConfiguration.class, CONFIGURATION, FORCE_RETURN_VALUES, NAME, MARSHALLER, MARSHALLER_CLASS, TEMPLATE_NAME, TRANSACTION_MODE, TRANSACTION_MANAGER);
   }

   private final NearCacheConfiguration nearCache;
   private final Attribute<String> configuration;
   private final Attribute<Boolean> forceReturnValues;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<Class> marshallerClass;
   private final Attribute<String> name;
   private final Attribute<String> templateName;
   private final Attribute<TransactionMode> transactionMode;
   private final Attribute<TransactionManagerLookup> transactionManager;

   RemoteCacheConfiguration(AttributeSet attributes, NearCacheConfiguration nearCache) {
      super("remote-cache", attributes, nearCache);
      this.nearCache = nearCache;
      configuration = attributes.attribute(CONFIGURATION);
      forceReturnValues = attributes.attribute(FORCE_RETURN_VALUES);
      name = attributes.attribute(NAME);
      marshaller = attributes.attribute(MARSHALLER);
      marshallerClass = attributes.attribute(MARSHALLER_CLASS);
      templateName = attributes.attribute(TEMPLATE_NAME);
      transactionMode = attributes.attribute(TRANSACTION_MODE);
      transactionManager = attributes.attribute(TRANSACTION_MANAGER);
   }

   public NearCacheConfiguration nearCache() {
      return nearCache;
   }

   public String configuration() {
      return configuration.get();
   }

   public boolean forceReturnValues() {
      return forceReturnValues.get();
   }

   public String name() {
      return name.get();
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   public Class<? extends Marshaller> marshallerClass() {
      return marshallerClass.get();
   }

   public String templateName() {
      return templateName.get();
   }

   public TransactionMode transactionMode() {
      return transactionMode.get();
   }

   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManager.get();
   }
}
