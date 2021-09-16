package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;

import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * @since 10.0
 */
public class CorsConfigurationBuilder implements Builder<CorsConfiguration> {
   private List<CorsRuleConfigurationBuilder> corsRules = new ArrayList<>();
   private List<CorsConfig> extraConfigs = new ArrayList<>();

   public CorsRuleConfigurationBuilder addNewRule() {
      CorsRuleConfigurationBuilder builder = new CorsRuleConfigurationBuilder();
      corsRules.add(builder);
      return builder;
   }

   public CorsConfigurationBuilder add(List<CorsConfig> corsConfig) {
      this.extraConfigs = corsConfig;
      return this;
   }

   @Override
   public CorsConfiguration create() {
      List<CorsRuleConfiguration> cors = corsRules.stream().distinct().map(CorsRuleConfigurationBuilder::create).collect(Collectors.toList());
      return new CorsConfiguration(cors, extraConfigs);
   }

   @Override
   public CorsConfigurationBuilder read(CorsConfiguration template) {
      corsRules.clear();
      template.corsRules().forEach(r -> addNewRule().read(r));
      return this;
   }
}
