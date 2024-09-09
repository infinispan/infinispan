package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration needed for State Transfer between different sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfiguration extends ConfigurationElement<XSiteStateTransferConfiguration> {
   public static final int DEFAULT_CHUNK_SIZE = 512;
   public static final TimeQuantity DEFAULT_TIMEOUT = TimeQuantity.valueOf("20m");
   public static final int DEFAULT_MAX_RETRIES = 30;
   public static final TimeQuantity DEFAULT_WAIT_TIME = TimeQuantity.valueOf("2s");

   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CHUNK_SIZE, DEFAULT_CHUNK_SIZE).immutable().build();
   public static final AttributeDefinition<TimeQuantity> TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TIMEOUT, DEFAULT_TIMEOUT).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<Integer> MAX_RETRIES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_RETRIES, DEFAULT_MAX_RETRIES).build();
   public static final AttributeDefinition<TimeQuantity> WAIT_TIME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.WAIT_TIME, DEFAULT_WAIT_TIME).parser(TimeQuantity.PARSER).build();
   public static final AttributeDefinition<XSiteStateTransferMode> MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODE, XSiteStateTransferMode.MANUAL).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(XSiteStateTransferConfiguration.class, CHUNK_SIZE, TIMEOUT, MAX_RETRIES, WAIT_TIME, MODE);
   }

   public XSiteStateTransferConfiguration(AttributeSet attributes) {
      super(Element.STATE_TRANSFER, attributes);
   }

   public int chunkSize() {
      return attributes.attribute(CHUNK_SIZE).get();
   }

   public long timeout() {
      return attributes.attribute(TIMEOUT).get().longValue();
   }

   public int maxRetries() {
      return attributes.attribute(MAX_RETRIES).get();
   }

   public long waitTime() {
      return attributes.attribute(WAIT_TIME).get().longValue();
   }

   public XSiteStateTransferMode mode() {
      return attributes.attribute(MODE).get();
   }
}
