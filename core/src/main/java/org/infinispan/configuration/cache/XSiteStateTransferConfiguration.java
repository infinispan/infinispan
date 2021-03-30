package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.STATE_TRANSFER;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Configuration needed for State Transfer between different sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfiguration implements Matchable<XSiteStateTransferConfiguration>, ConfigurationInfo {
   public static final int DEFAULT_CHUNK_SIZE = 512;
   public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(20);
   public static final int DEFAULT_MAX_RETRIES = 30;
   public static final long DEFAULT_WAIT_TIME = TimeUnit.SECONDS.toMillis(2);

   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder("chunkSize", DEFAULT_CHUNK_SIZE).immutable().build();
   public static final AttributeDefinition<Long> TIMEOUT = AttributeDefinition.builder("timeout", DEFAULT_TIMEOUT).build();
   public static final AttributeDefinition<Integer> MAX_RETRIES = AttributeDefinition.builder("maxRetries", DEFAULT_MAX_RETRIES).build();
   public static final AttributeDefinition<Long> WAIT_TIME = AttributeDefinition.builder("waitTime", DEFAULT_WAIT_TIME).build();
   public static final AttributeDefinition<XSiteStateTransferMode> MODE = AttributeDefinition.builder("mode", XSiteStateTransferMode.MANUAL).build();

   static final ElementDefinition<XSiteStateTransferConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(STATE_TRANSFER.getLocalName());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(XSiteStateTransferConfiguration.class, CHUNK_SIZE, TIMEOUT, MAX_RETRIES, WAIT_TIME, MODE);
   }

   @Override
   public ElementDefinition<XSiteStateTransferConfiguration> getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   private final Attribute<Integer> chunkSize;
   private final Attribute<Long> timeout;
   private final Attribute<Integer> maxRetries;
   private final Attribute<Long> waitTime;
   private final AttributeSet attributes;

   public XSiteStateTransferConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      chunkSize = attributes.attribute(CHUNK_SIZE);
      timeout = attributes.attribute(TIMEOUT);
      maxRetries = attributes.attribute(MAX_RETRIES);
      waitTime = attributes.attribute(WAIT_TIME);
   }

   public int chunkSize() {
      return chunkSize.get();
   }

   public long timeout() {
      return timeout.get();
   }

   public int maxRetries() {
      return maxRetries.get();
   }

   public long waitTime() {
      return waitTime.get();
   }

   public XSiteStateTransferMode mode() {
      return attributes.attribute(MODE).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      XSiteStateTransferConfiguration that = (XSiteStateTransferConfiguration) o;
      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return Objects.hash(attributes);
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfiguration [attributes=" + attributes + "]";
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
