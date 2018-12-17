package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.JMXStatisticsConfiguration.AVAILABLE;
import static org.infinispan.configuration.cache.JMXStatisticsConfiguration.ELEMENT_DEFINITION;
import static org.infinispan.configuration.cache.JMXStatisticsConfiguration.ENABLED;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * Determines whether statistics are gather and reported.
 *
 * @author pmuir
 *
 */
public class JMXStatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<JMXStatisticsConfiguration>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final AttributeSet attributes;

   JMXStatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = JMXStatisticsConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   /**
    * Enable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enable or disable statistics gathering and reporting
    */
   public JMXStatisticsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    * @param available
    * @return
    */
   public JMXStatisticsConfigurationBuilder available(boolean available) {
      attributes.attribute(AVAILABLE).set(available);
      return this;
   }

   @Override
   public void validate() {
      Attribute<Boolean> enabled = attributes.attribute(ENABLED);
      Attribute<Boolean> available = attributes.attribute(AVAILABLE);
      if (enabled.isModified() && available.isModified()) {
         if (enabled.get() && !available.get()) {
            throw log.statisticsEnabledNotAvailable();
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public JMXStatisticsConfiguration create() {
      return new JMXStatisticsConfiguration(attributes.protect());
   }

   @Override
   public JMXStatisticsConfigurationBuilder read(JMXStatisticsConfiguration template) {
      this.attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "JMXStatisticsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
