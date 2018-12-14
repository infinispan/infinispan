package org.infinispan.lock.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.lock.logging.Log;

/**
 * The {@link org.infinispan.lock.api.ClusteredLockManager} configuration builder.
 * <p>
 * It configures the number of owner and the {@link Reliability}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public class ClusteredLockManagerConfigurationBuilder implements Builder<ClusteredLockManagerConfiguration> {

   private static final ClusteredLockManagerConfiguration DEFAULT = new ClusteredLockManagerConfigurationBuilder(null).create();

   private static final Log log = LogFactory.getLog(ClusteredLockManagerConfigurationBuilder.class, Log.class);
   private final AttributeSet attributes = ClusteredLockManagerConfiguration.attributeDefinitionSet();
   private final List<ClusteredLockConfigurationBuilder> locksConfig = new ArrayList<>();

   private final GlobalConfigurationBuilder builder;

   public ClusteredLockManagerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
   }

   /**
    * @return the default {@link ClusteredLockManagerConfiguration}.
    */
   public static ClusteredLockManagerConfiguration defaultConfiguration() {
      return DEFAULT;
   }

   /**
    * Sets the number of copies of the counter's value available in the cluster.
    * <p>
    * A higher value will provide better availability at the cost of more expensive updates.
    * <p>
    * Default value is 2.
    *
    * @param numOwners the number of copies.
    */
   public ClusteredLockManagerConfigurationBuilder numOwner(int numOwners) {
      attributes.attribute(ClusteredLockManagerConfiguration.NUM_OWNERS).set(numOwners);
      return this;
   }

   /**
    * Sets the {@link Reliability} mode.
    * <p>
    * Default value is {@link Reliability#AVAILABLE}.
    *
    * @param reliability the {@link Reliability} mode.
    * @see Reliability
    */
   public ClusteredLockManagerConfigurationBuilder reliability(Reliability reliability) {
      attributes.attribute(ClusteredLockManagerConfiguration.RELIABILITY).set(reliability);
      return this;
   }

   @Override
   public void validate() {
      attributes.attributes().forEach(Attribute::validate);
   }

   @Override
   public ClusteredLockManagerConfiguration create() {
      Map<String, ClusteredLockConfiguration> clusteredLocks = new HashMap<>(locksConfig.size());
      for (ClusteredLockConfigurationBuilder builder : locksConfig) {
         ClusteredLockConfiguration lockConfiguration = builder.create();
         clusteredLocks.put(lockConfiguration.name(), lockConfiguration);
      }
      return new ClusteredLockManagerConfiguration(attributes.protect(), clusteredLocks);
   }

   @Override
   public Builder<?> read(ClusteredLockManagerConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   public ClusteredLockConfigurationBuilder addClusteredLock() {
      ClusteredLockConfigurationBuilder builder = new ClusteredLockConfigurationBuilder();
      locksConfig.add(builder);
      return builder;
   }
}
