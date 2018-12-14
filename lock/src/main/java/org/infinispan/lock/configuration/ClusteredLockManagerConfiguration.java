package org.infinispan.lock.configuration;

import java.util.Map;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.lock.logging.Log;

/**
 * The {@link org.infinispan.lock.api.ClusteredLockManager} configuration.
 * <p>
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public class ClusteredLockManagerConfiguration {

   private static final Log log = LogFactory.getLog(ClusteredLockManagerConfiguration.class, Log.class);
   static final AttributeDefinition<Reliability> RELIABILITY = AttributeDefinition
         .builder("reliability", Reliability.CONSISTENT)
         .validator(value -> {
            if (value == null) {
               throw log.invalidReliabilityMode();
            }
         })
         .immutable().build();

   static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder("numOwners", -1)
         .validator(value -> {
            if (value <= 0 && value != -1) {
               throw log.invalidNumOwners(value);
            }
         })
         .immutable().build();

   private final AttributeSet attributes;

   private Map<String, ClusteredLockConfiguration> locks;

   ClusteredLockManagerConfiguration(AttributeSet attributes, Map<String, ClusteredLockConfiguration> locks) {
      this.attributes = attributes;
      this.locks = locks;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteredLockManagerConfiguration.class, NUM_OWNERS, RELIABILITY);
   }

   public int numOwners() {
      return attributes.attribute(NUM_OWNERS).get();
   }

   public Reliability reliability() {
      return attributes.attribute(RELIABILITY).get();
   }

   AttributeSet attributes() {
      return attributes;
   }

   public Map<String, ClusteredLockConfiguration> locks() {
      return locks;
   }
}
