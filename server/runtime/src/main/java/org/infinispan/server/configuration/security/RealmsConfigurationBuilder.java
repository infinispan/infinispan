package org.infinispan.server.configuration.security;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class RealmsConfigurationBuilder implements Builder<RealmsConfiguration> {

   private final Map<String, RealmConfigurationBuilder> securityRealms = new LinkedHashMap<>(2);

   public RealmsConfigurationBuilder() {
   }

   public RealmConfigurationBuilder addSecurityRealm(String name) {
      RealmConfigurationBuilder realmConfigurationBuilder = new RealmConfigurationBuilder(name);
      securityRealms.put(name, realmConfigurationBuilder);
      return realmConfigurationBuilder;
   }

   @Override
   public RealmsConfiguration create() {
      Map<String, RealmConfiguration> map = securityRealms.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().create()));
      return new RealmsConfiguration(map);
   }

   @Override
   public RealmsConfigurationBuilder read(RealmsConfiguration template) {
      securityRealms.clear();
      template.realms().entrySet().forEach(e ->  addSecurityRealm(e.getKey()).read(e.getValue()));
      return this;
   }

   @Override
   public void validate() {
      securityRealms.values().forEach(RealmConfigurationBuilder::validate);
   }
}
