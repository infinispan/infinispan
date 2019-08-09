package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * @since 10.0
 */
public class CorsConfiguration implements ConfigurationInfo {
   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("cors-rules");
   private final List<CorsConfig> nettyCorsConfigs;

   private List<CorsRuleConfiguration> corsConfigurations;
   private List<ConfigurationInfo> elements = new ArrayList<>();

   CorsConfiguration(List<CorsRuleConfiguration> corsConfigurations, List<CorsConfig> extraConfigs) {
      this.corsConfigurations = corsConfigurations;
      this.nettyCorsConfigs = corsConfigurations.stream().map(CorsRuleConfiguration::corsConfig).collect(Collectors.toList());
      this.nettyCorsConfigs.addAll(extraConfigs);
      elements.addAll(corsConfigurations);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   List<CorsRuleConfiguration> corsRules() {
      return corsConfigurations;
   }

   List<CorsConfig> corsConfigs() {
      return nettyCorsConfigs;
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CorsConfiguration that = (CorsConfiguration) o;

      return corsConfigurations.equals(that.corsConfigurations);
   }

   @Override
   public int hashCode() {
      return corsConfigurations.hashCode();
   }

   @Override
   public String toString() {
      return "CorsRulesConfiguration{" +
            "corsConfigurations=" + corsConfigurations +
            '}';
   }
}
