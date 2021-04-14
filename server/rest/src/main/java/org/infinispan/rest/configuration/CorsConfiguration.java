package org.infinispan.rest.configuration;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * @since 10.0
 */
public class CorsConfiguration extends ConfigurationElement<CorsConfiguration> {
   private final List<CorsConfig> nettyCorsConfigs;

   private List<CorsRuleConfiguration> corsConfigurations;

   CorsConfiguration(List<CorsRuleConfiguration> corsConfigurations, List<CorsConfig> extraConfigs) {
      super("cors-rules", AttributeSet.EMPTY, children(corsConfigurations));
      this.corsConfigurations = corsConfigurations;
      this.nettyCorsConfigs = corsConfigurations.stream().map(CorsRuleConfiguration::corsConfig).collect(Collectors.toList());
      this.nettyCorsConfigs.addAll(extraConfigs);
   }

   public List<CorsRuleConfiguration> corsRules() {
      return corsConfigurations;
   }

   List<CorsConfig> corsConfigs() {
      return Collections.unmodifiableList(nettyCorsConfigs);
   }
}
