package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.DistributedRealmConfiguration.NAME;
import static org.infinispan.server.configuration.security.DistributedRealmConfiguration.REALMS;

import java.util.Arrays;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class DistributedRealmConfigurationBuilder implements RealmProviderBuilder<DistributedRealmConfiguration> {

   private final AttributeSet attributes;

   public DistributedRealmConfigurationBuilder() {
      this.attributes = DistributedRealmConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public DistributedRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   public DistributedRealmConfigurationBuilder realms(String[] realms) {
      attributes.attribute(REALMS).set(Arrays.asList(realms));
      return this;
   }

   @Override
   public DistributedRealmConfiguration create() {
      return new DistributedRealmConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(DistributedRealmConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 1; // Must be the last
   }
}
