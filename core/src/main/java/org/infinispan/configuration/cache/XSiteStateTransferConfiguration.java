package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration needed for State Transfer between different sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfiguration extends ConfigurationElement<XSiteStateTransferConfiguration> {
   public static final int DEFAULT_CHUNK_SIZE = 512;
   public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(20);
   public static final int DEFAULT_MAX_RETRIES = 30;
   public static final long DEFAULT_WAIT_TIME = TimeUnit.SECONDS.toMillis(2);

   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CHUNK_SIZE, DEFAULT_CHUNK_SIZE).immutable().build();
   public static final AttributeDefinition<Long> TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TIMEOUT, DEFAULT_TIMEOUT).build();
   public static final AttributeDefinition<Integer> MAX_RETRIES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_RETRIES, DEFAULT_MAX_RETRIES).build();
   public static final AttributeDefinition<Long> WAIT_TIME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.WAIT_TIME, DEFAULT_WAIT_TIME).build();
   public static final AttributeDefinition<XSiteStateTransferMode> MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODE, XSiteStateTransferMode.MANUAL).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(XSiteStateTransferConfiguration.class, CHUNK_SIZE, TIMEOUT, MAX_RETRIES, WAIT_TIME, MODE);
   }

   private final Attribute<Integer> chunkSize;
   private final Attribute<Long> timeout;
   private final Attribute<Integer> maxRetries;
   private final Attribute<Long> waitTime;

   public XSiteStateTransferConfiguration(AttributeSet attributes) {
      super(Element.STATE_TRANSFER, attributes);
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
}
