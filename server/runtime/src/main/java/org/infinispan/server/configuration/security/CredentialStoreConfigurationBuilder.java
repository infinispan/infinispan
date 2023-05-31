package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.CREDENTIAL;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.NAME;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.PATH;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.RELATIVE_TO;
import static org.infinispan.server.configuration.security.CredentialStoreConfiguration.TYPE;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class CredentialStoreConfigurationBuilder implements Builder<CredentialStoreConfiguration> {
   private final AttributeSet attributes;

   CredentialStoreConfigurationBuilder(String name) {
      this.attributes = CredentialStoreConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public CredentialStoreConfigurationBuilder path(String value) {
      attributes.attribute(PATH).set(value);
      return this;
   }

   public CredentialStoreConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public CredentialStoreConfigurationBuilder type(String type) {
      attributes.attribute(TYPE).set(type);
      return this;
   }

   public CredentialStoreConfigurationBuilder credential(Supplier<CredentialSource> credential) {
      attributes.attribute(CREDENTIAL).set(credential);
      return this;
   }

   @Override
   public CredentialStoreConfiguration create() {
      return new CredentialStoreConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(CredentialStoreConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }
}
