package org.infinispan.server.configuration.security;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.Server;
import org.infinispan.server.security.ServerSecurityRealm;

/**
 * @since 10.0
 */
public class RealmsConfigurationBuilder implements Builder<RealmsConfiguration> {

   private final Map<String, RealmConfigurationBuilder> securityRealms = new LinkedHashMap<>(2);

   public RealmsConfigurationBuilder() {
   }

   public RealmConfigurationBuilder addSecurityRealm(String name) {
      RealmConfigurationBuilder realmConfigurationBuilder = new RealmConfigurationBuilder(name, this);
      securityRealms.put(name, realmConfigurationBuilder);
      return realmConfigurationBuilder;
   }

   public ServerSecurityRealm getServerSecurityRealm(String name) {
      RealmConfigurationBuilder builder = securityRealms.get(name);
      return builder.getServerSecurityRealm();
   }

   public SSLContext getSSLContext(String name) {
      RealmConfigurationBuilder realmConfigurationBuilder = securityRealms.get(name);
      if (realmConfigurationBuilder == null) {
         throw Server.log.unknownSecurityDomain(name);
      }
      return realmConfigurationBuilder.getSSLContext();
   }

   public SSLContext getClientSSLContext(String name) {
      RealmConfigurationBuilder realmConfigurationBuilder = securityRealms.get(name);
      if (realmConfigurationBuilder == null) {
         throw Server.log.unknownSecurityDomain(name);
      }
      return realmConfigurationBuilder.getClientSSLContext();
   }

   @Override
   public RealmsConfiguration create() {
      List<RealmConfiguration> realms = securityRealms.values().stream().map(RealmConfigurationBuilder::create).collect(Collectors.toList());
      return new RealmsConfiguration(realms);
   }

   @Override
   public RealmsConfigurationBuilder read(RealmsConfiguration template) {
      securityRealms.clear();
      template.realms().forEach(r -> addSecurityRealm(r.name()).read(r));
      return this;
   }

   @Override
   public void validate() {
      securityRealms.values().forEach(RealmConfigurationBuilder::validate);
   }
}
