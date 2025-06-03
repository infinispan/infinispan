package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.near.DefaultNearCacheFactory;
import org.infinispan.client.hotrod.near.NearCacheFactory;
import org.infinispan.client.hotrod.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@BuiltBy(RemoteCacheConfigurationBuilder.class)
public class RemoteCacheConfiguration {
   public static final AttributeDefinition<String> CONFIGURATION = AttributeDefinition.builder("configuration", null, String.class).build();
   public static final AttributeDefinition<Boolean> FORCE_RETURN_VALUES = AttributeDefinition.builder("force-return-values", false, Boolean.class).build();
   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   public static final AttributeDefinition<NearCacheMode> NEAR_CACHE_MODE = AttributeDefinition.builder("near-cache-mode", NearCacheMode.DISABLED).build();
   public static final AttributeDefinition<Integer> NEAR_CACHE_MAX_ENTRIES = AttributeDefinition.builder("near-cache-max-entries", -1).build();
   public static final AttributeDefinition<Boolean> NEAR_CACHE_BLOOM_FILTER = AttributeDefinition.builder("near-cache-bloom-filter", false).build();
   public static final AttributeDefinition<NearCacheFactory> NEAR_CACHE_FACTORY = AttributeDefinition.builder("near-cache-factory", DefaultNearCacheFactory.INSTANCE, NearCacheFactory.class).build();
   public static final AttributeDefinition<String> TEMPLATE_NAME = AttributeDefinition.builder("template-name", null, String.class).build();
   public static final AttributeDefinition<TransactionMode> TRANSACTION_MODE = AttributeDefinition.builder("transaction-mode", TransactionMode.NONE).build();
   public static final AttributeDefinition<TransactionManagerLookup> TRANSACTION_MANAGER = AttributeDefinition.builder("transaction-manager", GenericTransactionManagerLookup.getInstance(), TransactionManagerLookup.class).build();
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class).build();
   public static final AttributeDefinition<Class> MARSHALLER_CLASS = AttributeDefinition.builder("marshallerClass", null, Class.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteCacheConfiguration.class, CONFIGURATION, FORCE_RETURN_VALUES, NAME, MARSHALLER, MARSHALLER_CLASS, NEAR_CACHE_MODE, NEAR_CACHE_MAX_ENTRIES, NEAR_CACHE_BLOOM_FILTER, NEAR_CACHE_FACTORY, TEMPLATE_NAME, TRANSACTION_MODE, TRANSACTION_MANAGER);
   }

   private final Attribute<String> configuration;
   private final Attribute<Boolean> forceReturnValues;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<Class> marshallerClass;
   private final Attribute<String> name;
   private final Attribute<NearCacheMode> nearCacheMode;
   private final Attribute<Integer> nearCacheMaxEntries;
   private final Attribute<Boolean> nearCacheBloomFilter;
   private final Attribute<String> templateName;
   private final Attribute<TransactionMode> transactionMode;
   private final Attribute<TransactionManagerLookup> transactionManager;
   private final AttributeSet attributes;

   RemoteCacheConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      configuration = attributes.attribute(CONFIGURATION);
      forceReturnValues = attributes.attribute(FORCE_RETURN_VALUES);
      name = attributes.attribute(NAME);
      marshaller = attributes.attribute(MARSHALLER);
      marshallerClass = attributes.attribute(MARSHALLER_CLASS);
      nearCacheMode = attributes.attribute(NEAR_CACHE_MODE);
      nearCacheMaxEntries = attributes.attribute(NEAR_CACHE_MAX_ENTRIES);
      nearCacheBloomFilter = attributes.attribute(NEAR_CACHE_BLOOM_FILTER);
      templateName = attributes.attribute(TEMPLATE_NAME);
      transactionMode = attributes.attribute(TRANSACTION_MODE);
      transactionManager = attributes.attribute(TRANSACTION_MANAGER);
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

   public NearCacheMode nearCacheMode() {
      return nearCacheMode.get();
   }

   public int nearCacheMaxEntries() {
      return nearCacheMaxEntries.get();
   }

   public boolean nearCacheBloomFilter() {
      return nearCacheBloomFilter.get();
   }

   public NearCacheFactory nearCacheFactory() {
      return attributes.attribute(NEAR_CACHE_FACTORY).get();
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

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "RemoteCacheConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      RemoteCacheConfiguration other = (RemoteCacheConfiguration) obj;
      if (attributes == null) {
         return other.attributes == null;
      } else return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }
}
