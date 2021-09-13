package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PASSWORD;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.PROVIDER;
import static org.infinispan.server.configuration.security.TrustStoreConfiguration.RELATIVE_TO;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.InstanceSupplier;

/**
 * @since 12.1
 */
public class TrustStoreConfigurationBuilder implements Builder<TrustStoreConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;

   TrustStoreConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = TrustStoreConfiguration.attributeDefinitionSet();
   }

   public TrustStoreConfigurationBuilder password(char[] password) {
      attributes.attribute(PASSWORD).set(new InstanceSupplier<>(password));
      return this;
   }

   public TrustStoreConfigurationBuilder password(Supplier<char[]> password) {
      attributes.attribute(PASSWORD).set(password);
      return this;
   }

   public TrustStoreConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public TrustStoreConfigurationBuilder provider(String value) {
      attributes.attribute(PROVIDER).set(value);
      return this;
   }

   public TrustStoreConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   @Override
   public TrustStoreConfiguration create() {
      return new TrustStoreConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreConfigurationBuilder read(TrustStoreConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
