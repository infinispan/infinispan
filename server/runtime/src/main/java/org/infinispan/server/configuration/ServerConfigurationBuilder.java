package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.configuration.endpoint.EndpointsConfigurationBuilder;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.configuration.security.SecurityConfigurationBuilder;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ServerConfigurationBuilder implements Builder<ServerConfiguration> {
   private final Properties properties = new Properties();
   private final InterfacesConfigurationBuilder interfaces = new InterfacesConfigurationBuilder();
   private final SocketBindingsConfigurationBuilder socketBindings = new SocketBindingsConfigurationBuilder(this);
   private final SecurityConfigurationBuilder security = new SecurityConfigurationBuilder(this);
   private final DataSourcesConfigurationBuilder dataSources = new DataSourcesConfigurationBuilder();
   private final EndpointsConfigurationBuilder endpoints = new EndpointsConfigurationBuilder(this);
   private final List<SSLContextSupplier> suppliers = new ArrayList<>();

   public ServerConfigurationBuilder(GlobalConfigurationBuilder builder) {
   }

   public ServerConfigurationBuilder properties(Properties properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
   }

   public Properties properties() {
      return properties;
   }

   public SecurityConfigurationBuilder security() {
      return security;
   }

   public InterfacesConfigurationBuilder interfaces() {
      return interfaces;
   }

   public SocketBindingsConfigurationBuilder socketBindings() {
      return socketBindings;
   }

   public DataSourcesConfigurationBuilder dataSources() {
      return dataSources;
   }

   public EndpointsConfigurationBuilder endpoints() {
      return endpoints;
   }

   @Override
   public void validate() {
      Arrays.asList(interfaces, socketBindings, security, endpoints).forEach(Builder::validate);
   }

   @Override
   public ServerConfiguration create() {
      SecurityConfiguration securityConfiguration = security.create();
      for(SSLContextSupplier supplier : suppliers) {
         supplier.configuration = securityConfiguration;
      }
      InterfacesConfiguration interfacesConfiguration = interfaces.create();
      SocketBindingsConfiguration bindingsConfiguration = socketBindings.create(interfacesConfiguration);
      return new ServerConfiguration(
            interfacesConfiguration,
            bindingsConfiguration,
            securityConfiguration,
            dataSources.create(),
            endpoints.create(bindingsConfiguration, securityConfiguration)
      );
   }

   @Override
   public Builder<?> read(ServerConfiguration template) {
      // Do nothing
      return this;
   }

   public Supplier<SSLContext> serverSSLContextSupplier(String sslContextName) {
      SSLContextSupplier supplier = new SSLContextSupplier(sslContextName, false);
      suppliers.add(supplier);
      return supplier;
   }

   public Supplier<SSLContext> clientSSLContextSupplier(String sslContextName) {
      SSLContextSupplier supplier = new SSLContextSupplier(sslContextName, true);
      suppliers.add(supplier);
      return supplier;
   }

   private static class SSLContextSupplier implements Supplier<SSLContext> {
      final String name;
      final boolean client;
      SecurityConfiguration configuration;

      SSLContextSupplier(String name, boolean client) {
         this.name = name;
         this.client = client;
      }

      @Override
      public SSLContext get() {
         return client ? configuration.realms().getRealm(name).clientSSLContext() : configuration.realms().getRealm(name).serverSSLContext();
      }
   }
}
