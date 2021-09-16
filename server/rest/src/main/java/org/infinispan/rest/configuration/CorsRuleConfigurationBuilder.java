package org.infinispan.rest.configuration;

import java.util.Arrays;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

/**
 * @since 10.0
 */
public class CorsRuleConfigurationBuilder implements Builder<CorsRuleConfiguration> {
   private static final String[] ALLOW_ALL_ORIGINS = {"*"};

   private final AttributeSet attributes;

   private final Attribute<String> name;
   private final Attribute<Long> maxAge;
   private final Attribute<Boolean> allowCredentials;
   private final Attribute<String[]> allowedHeaders;
   private final Attribute<String[]> allowedOrigins;
   private final Attribute<String[]> allowedMethods;
   private final Attribute<String[]> exposeHeaders;

   CorsRuleConfigurationBuilder() {
      this.attributes = CorsRuleConfiguration.attributeDefinitionSet();
      name = attributes.attribute(CorsRuleConfiguration.NAME);
      maxAge = attributes.attribute(CorsRuleConfiguration.MAX_AGE);
      allowCredentials = attributes.attribute(CorsRuleConfiguration.ALLOW_CREDENTIALS);
      allowedHeaders = attributes.attribute(CorsRuleConfiguration.ALLOW_HEADERS);
      allowedOrigins = attributes.attribute(CorsRuleConfiguration.ALLOW_ORIGINS);
      allowedMethods = attributes.attribute(CorsRuleConfiguration.ALLOW_METHODS);
      exposeHeaders = attributes.attribute(CorsRuleConfiguration.EXPOSE_HEADERS);
   }

   public CorsRuleConfigurationBuilder name(String value) {
      name.set(value);
      return this;
   }

   public CorsRuleConfigurationBuilder allowCredentials(boolean allow) {
      allowCredentials.set(allow);
      return this;
   }

   public CorsRuleConfigurationBuilder maxAge(long value) {
      maxAge.set(value);
      return this;
   }

   public CorsRuleConfigurationBuilder allowOrigins(String[] values) {
      allowedOrigins.set(values);
      return this;
   }

   public CorsRuleConfigurationBuilder allowMethods(String[] values) {
      allowedMethods.set(values);
      return this;
   }

   public CorsRuleConfigurationBuilder allowHeaders(String[] values) {
      allowedHeaders.set(values);
      return this;
   }

   public CorsRuleConfigurationBuilder exposeHeaders(String[] values) {
      exposeHeaders.set(values);
      return this;
   }

   @Override
   public CorsRuleConfiguration create() {
      CorsConfig corsConfig = createCors();
      return new CorsRuleConfiguration(attributes.protect(), corsConfig);
   }

   private CorsConfig createCors() {
      boolean isAllowAll = Arrays.equals(allowedOrigins.get(), ALLOW_ALL_ORIGINS);
      CorsConfigBuilder builder = CorsConfigBuilder.forAnyOrigin();
      if (allowedOrigins.isModified() && !isAllowAll) {
         builder = CorsConfigBuilder.forOrigins(allowedOrigins.get());
      }

      if (allowCredentials.isModified() && allowCredentials.get() != null) {
         if (allowCredentials.get()) builder.allowCredentials();
      }

      if (maxAge.isModified()) {
         builder.maxAge(maxAge.get());
      }

      if (allowedHeaders.isModified()) {
         builder.allowedRequestHeaders(allowedHeaders.get());
      }

      if (allowedMethods.isModified()) {
         HttpMethod[] methods = Arrays.stream(allowedMethods.get()).map(HttpMethod::valueOf).toArray(HttpMethod[]::new);
         builder.allowedRequestMethods(methods);
      }

      if (exposeHeaders.isModified()) {
         builder.exposeHeaders(exposeHeaders.get());
      }

      return builder.build();
   }

   @Override
   public CorsRuleConfigurationBuilder read(CorsRuleConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
