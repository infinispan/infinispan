package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.ServerConfigurationParser.NAMESPACE;
import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.configuration.endpoint.EndpointConfiguration;
import org.infinispan.server.configuration.endpoint.EndpointsConfiguration;
import org.infinispan.server.configuration.security.AggregateRealmConfiguration;
import org.infinispan.server.configuration.security.CredentialStoreConfiguration;
import org.infinispan.server.configuration.security.CredentialStoresConfiguration;
import org.infinispan.server.configuration.security.CredentialStoresConfigurationBuilder;
import org.infinispan.server.configuration.security.DistributedRealmConfiguration;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration;
import org.infinispan.server.configuration.security.LdapAttributeConfiguration;
import org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration;
import org.infinispan.server.configuration.security.LdapRealmConfiguration;
import org.infinispan.server.configuration.security.LocalRealmConfiguration;
import org.infinispan.server.configuration.security.PropertiesRealmConfiguration;
import org.infinispan.server.configuration.security.RealmConfiguration;
import org.infinispan.server.configuration.security.RealmProvider;
import org.infinispan.server.configuration.security.RealmsConfiguration;
import org.infinispan.server.configuration.security.SSLConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.configuration.security.ServerIdentitiesConfiguration;
import org.infinispan.server.configuration.security.TokenRealmConfiguration;
import org.infinispan.server.configuration.security.TrustStoreConfiguration;
import org.infinispan.server.configuration.security.TrustStoreRealmConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.security.PasswordCredentialSource;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.util.RegexNameRewriter;
import org.wildfly.security.credential.source.CredentialSource;

public class ServerConfigurationSerializer
      implements ConfigurationSerializer<ServerConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, ServerConfiguration configuration) {
      writer.writeStartElement(Element.SERVER);
      writer.writeDefaultNamespace(NAMESPACE + Version.getMajorMinor());
      writeInterfaces(writer, configuration.interfaces);
      writeSocketBindings(writer, configuration.socketBindings);
      writeSecurity(writer, configuration.security());
      writeDataSources(writer, configuration.dataSources());
      writeEndpoints(writer, configuration.endpoints());
      writer.writeEndElement();
   }

   private void writeInterfaces(ConfigurationWriter writer, InterfacesConfiguration networkInterfaces) {
      writer.writeStartListElement(Element.INTERFACES, true);
      for (InterfaceConfiguration netIf : networkInterfaces.interfaces().values()) {
         writer.writeStartElement(Element.INTERFACE);
         writer.writeAttribute(Attribute.NAME, netIf.name());
         AddressType addressType = netIf.addressConfiguration().addressType();
         if (addressType.hasValue()) {
            writer.writeStartElement(addressType);
            writer.writeAttribute(Attribute.VALUE, netIf.addressConfiguration().value());
            writer.writeEndElement();
         } else {
            writer.writeEmptyElement(addressType);
         }
         writer.writeEndElement();
      }
      writer.writeEndListElement();
   }

   private void writeSocketBindings(ConfigurationWriter writer, SocketBindingsConfiguration socketBindings) {
      if (writer.hasFeature(ConfigurationFormatFeature.MIXED_ELEMENTS)) {
         writer.writeStartElement(Element.SOCKET_BINDINGS);
         socketBindings.attributes().write(writer);
         for (SocketBindingConfiguration socketBinding : socketBindings.socketBindings().values()) {
            writer.writeStartElement(Element.SOCKET_BINDING);
            socketBinding.attributes().write(writer);
            writer.writeEndElement(); // SOCKET_BINDING
         }
         writer.writeEndElement(); // SOCKET_BINDINGS
      } else {
         writer.writeStartElement(Element.SOCKET_BINDINGS);
         socketBindings.attributes().write(writer);
         writer.writeStartArrayElement(Element.SOCKET_BINDING);
         for (SocketBindingConfiguration socketBinding : socketBindings.socketBindings().values()) {
            socketBinding.write(writer);
         }
         writer.writeEndArrayElement(); // SOCKET_BINDING
         writer.writeEndElement(); // SOCKET_BINDINGS
      }
   }

   private void writeSecurity(ConfigurationWriter writer, SecurityConfiguration security) {
      writer.writeStartElement(Element.SECURITY);
      writeCredentialStores(writer, security.credentialStores());
      writeSecurityRealms(writer, security.realms());
      writer.writeEndElement();
   }

   private void writeCredentialStores(ConfigurationWriter writer, CredentialStoresConfiguration credentialStores) {
      if (!credentialStores.credentialStores().isEmpty()) {
         writer.writeStartArrayElement(Element.CREDENTIAL_STORES);
         for (CredentialStoreConfiguration credentialStore : credentialStores.credentialStores().values()) {
            credentialStore.write(writer);
         }
         writer.writeEndArrayElement();
      }
   }

   private void writeSecurityRealms(ConfigurationWriter writer, RealmsConfiguration realms) {
      if (!realms.realms().isEmpty()) {
         writer.writeStartArrayElement(Element.SECURITY_REALMS);
         for (Map.Entry<String, RealmConfiguration> e : realms.realms().entrySet()) {
            RealmConfiguration realm = e.getValue();
            writer.writeStartElement(Element.SECURITY_REALM);
            realm.attributes().write(writer);
            writeServerIdentities(writer, realm.serverIdentitiesConfiguration());
            for (RealmProvider provider : realm.realmProviders()) {
               if (provider instanceof LdapRealmConfiguration) {
                  writeRealm(writer, (LdapRealmConfiguration) provider);
               } else if (provider instanceof LocalRealmConfiguration) {
                  writeRealm(writer, (LocalRealmConfiguration) provider);
               } else if (provider instanceof PropertiesRealmConfiguration) {
                  writeRealm(writer, (PropertiesRealmConfiguration) provider);
               } else if (provider instanceof TokenRealmConfiguration) {
                  writeRealm(writer, (TokenRealmConfiguration) provider);
               } else if (provider instanceof TrustStoreConfiguration) {
                  writeRealm(writer, (TrustStoreRealmConfiguration) provider);
               } else if (provider instanceof DistributedRealmConfiguration) {
                  writeRealm(writer, (DistributedRealmConfiguration) provider);
               } else if (provider instanceof AggregateRealmConfiguration) {
                  writeRealm(writer, (AggregateRealmConfiguration) provider);
               }
            }
            writer.writeEndElement(); // SECURITY_REALM
         }
         writer.writeEndArrayElement(); // SECURITY_REALMS
      }
   }

   private void writeRealm(ConfigurationWriter writer, LdapRealmConfiguration realm) {
      if (realm.name() != null) {
         writer.writeStartElement(Element.LDAP_REALM);
         realm.attributes().write(writer);
         NameRewriter nameRewriter = realm.nameRewriter();
         if (nameRewriter != null) {
            writer.writeStartElement(Element.NAME_REWRITER);
            if (nameRewriter instanceof RegexNameRewriter) {
               RegexNameRewriter regexNameRewriter = (RegexNameRewriter) nameRewriter;
               writer.writeStartElement(Element.REGEX_PRINCIPAL_TRANSFORMER);
               writer.writeAttribute(Attribute.PATTERN, regexNameRewriter.getPattern().pattern());
               writer.writeAttribute(Attribute.REPLACEMENT, regexNameRewriter.getReplacement());
               writer.writeEndElement();
            } else {
               throw new IllegalArgumentException();
            }
            writer.writeEndElement();
         }
         LdapIdentityMappingConfiguration identity = realm.identityMapping();
         writer.writeStartElement(Element.IDENTITY_MAPPING);
         identity.attributes().write(writer);
         if (!identity.attributeMappings().isEmpty()) {
            writer.writeStartElement(Element.ATTRIBUTE_MAPPING);
            for (LdapAttributeConfiguration mapping : identity.attributeMappings()) {
               mapping.write(writer);
            }
            writer.writeEndElement(); // ATTRIBUTE_MAPPING
         }
         identity.userPasswordMapper().attributes().write(writer, Element.USER_PASSWORD_MAPPER);
         writer.writeEndElement(); // IDENTITY_MAPPING
         writer.writeEndElement(); // LDAP_REALM
      }
   }

   private void writeRealm(ConfigurationWriter writer, LocalRealmConfiguration realm) {
      realm.attributes().write(writer, Element.LOCAL_REALM);
   }

   private void writeRealm(ConfigurationWriter writer, TokenRealmConfiguration realm) {
      if (realm.name() != null) {
         writer.writeStartElement(Element.TOKEN_REALM);
         realm.attributes().write(writer);
         realm.jwtConfiguration().attributes().write(writer, Element.JWT);
         realm.oauth2Configuration().attributes().write(writer, Element.OAUTH2_INTROSPECTION);
         writer.writeEndElement();
      }
   }

   private void writeRealm(ConfigurationWriter writer, PropertiesRealmConfiguration realm) {
      if (realm.userProperties().digestRealmName() != null) {
         writer.writeStartElement(Element.PROPERTIES_REALM);
         realm.attributes().write(writer);
         realm.userProperties().attributes().write(writer, Element.USER_PROPERTIES);
         realm.groupProperties().attributes().write(writer, Element.GROUP_PROPERTIES);
         writer.writeEndElement();
      }
   }

   private void writeRealm(ConfigurationWriter writer, TrustStoreRealmConfiguration realm) {
      realm.write(writer);
   }

   private void writeRealm(ConfigurationWriter writer, AggregateRealmConfiguration realm) {
      realm.write(writer);
   }

   private void writeRealm(ConfigurationWriter writer, DistributedRealmConfiguration realm) {
      realm.write(writer);
   }

   private void writeServerIdentities(ConfigurationWriter writer, ServerIdentitiesConfiguration identities) {
      SSLConfiguration ssl = identities.sslConfiguration();
      List<KerberosSecurityFactoryConfiguration> kerberosList = identities.kerberosConfigurations();
      if (ssl != null || !kerberosList.isEmpty()) {
         writer.writeStartElement(Element.SERVER_IDENTITIES);
         if (ssl != null) {
            writer.writeStartElement(Element.SSL);
            ssl.keyStore().write(writer);
            TrustStoreConfiguration trustStore = ssl.trustStore();
            if (trustStore != null) {
               trustStore.write(writer);
            }
            ssl.engine().write(writer);
            writer.writeEndElement();
         }

         if (!kerberosList.isEmpty()) {
            for (KerberosSecurityFactoryConfiguration kerberos : kerberosList) {
               kerberos.write(writer);
            }
         }
         writer.writeEndElement();
      }
   }

   private void writeDataSources(ConfigurationWriter writer, Map<String, DataSourceConfiguration> dataSources) {
      if (!dataSources.isEmpty()) {
         writer.writeStartListElement(Element.DATA_SOURCES, true);
         for (Map.Entry<String, DataSourceConfiguration> configuration : dataSources.entrySet()) {
            AttributeSet attributes = configuration.getValue().attributes();
            writer.writeStartElement(Element.DATA_SOURCE);
            attributes.write(writer, DataSourceConfiguration.NAME);
            attributes.write(writer, DataSourceConfiguration.JNDI_NAME);
            attributes.write(writer, DataSourceConfiguration.STATISTICS);
            writer.writeStartElement(Element.CONNECTION_FACTORY);
            attributes.write(writer, DataSourceConfiguration.DRIVER);
            attributes.write(writer, DataSourceConfiguration.USERNAME);
            attributes.write(writer, DataSourceConfiguration.URL);
            attributes.write(writer, DataSourceConfiguration.INITIAL_SQL);
            attributes.write(writer, DataSourceConfiguration.PASSWORD);
            attributes.write(writer, DataSourceConfiguration.CONNECTION_PROPERTIES);
            writer.writeEndElement(); // Element.CONNECTION_FACTORY
            writer.writeStartElement(Element.CONNECTION_POOL);
            attributes.write(writer, DataSourceConfiguration.BACKGROUND_VALIDATION);
            attributes.write(writer, DataSourceConfiguration.BLOCKING_TIMEOUT);
            attributes.write(writer, DataSourceConfiguration.IDLE_REMOVAL);
            attributes.write(writer, DataSourceConfiguration.INITIAL_SIZE);
            attributes.write(writer, DataSourceConfiguration.LEAK_DETECTION);
            attributes.write(writer, DataSourceConfiguration.MAX_SIZE);
            attributes.write(writer, DataSourceConfiguration.MIN_SIZE);
            writer.writeEndElement(); // Element.CONNECTION_POOL
            writer.writeEndElement(); // Element.DATA_SOURCE
         }
         writer.writeEndListElement();
      }
   }

   private void writeEndpoints(ConfigurationWriter writer, EndpointsConfiguration endpoints) {
      writer.writeStartElement(Element.ENDPOINTS);
      for (EndpointConfiguration endpoint : endpoints.endpoints()) {
         writer.writeStartElement(Element.ENDPOINT);
         endpoint.attributes().write(writer);
         for (ProtocolServerConfiguration connector : endpoint.connectors()) {
            if (connector instanceof HotRodServerConfiguration) {
               writeConnector(writer, (HotRodServerConfiguration) connector);
            } else if (connector instanceof RestServerConfiguration) {
               writeConnector(writer, (RestServerConfiguration) connector);
            } else if (connector instanceof MemcachedServerConfiguration) {
               writeConnector(writer, (MemcachedServerConfiguration) connector);
            }
         }
         writer.writeEndElement();
      }
      writer.writeEndElement();
   }

   private void writeConnector(ConfigurationWriter writer, HotRodServerConfiguration connector) {
      if (connector.isImplicit()) {
         return;
      }
      writer.writeStartElement(org.infinispan.server.hotrod.configuration.Element.HOTROD_CONNECTOR);
      connector.attributes().write(writer);
      connector.topologyCache().write(writer);
      if (connector.authentication().enabled()) {
         writer.writeStartElement(org.infinispan.server.hotrod.configuration.Element.AUTHENTICATION);
         connector.authentication().attributes().write(writer);
         connector.authentication().sasl().write(writer);
         writer.writeEndElement();
      }
      connector.encryption().write(writer);
      writer.writeEndElement();
   }

   private void writeConnector(ConfigurationWriter writer, RestServerConfiguration connector) {
      if (connector.isImplicit()) {
         return;
      }
      writer.writeStartElement(org.infinispan.server.configuration.rest.Element.REST_CONNECTOR);
      connector.attributes().write(writer);
      if (connector.authentication().enabled()) {
         writer.writeStartElement(org.infinispan.server.configuration.rest.Element.AUTHENTICATION);
         connector.authentication().attributes().write(writer);
         writer.writeEndElement();
      }
      connector.cors().write(writer);
      connector.encryption().write(writer);
      writer.writeEndElement();
   }

   private void writeConnector(ConfigurationWriter writer, MemcachedServerConfiguration connector) {
      if (connector.isImplicit()) {
         return;
      }
      connector.write(writer);
   }

   public static AttributeSerializer<Supplier<CredentialSource>> CREDENTIAL = (writer, name, value) -> {
      if (value instanceof PasswordCredentialSource) {
         String credential = writer.clearTextSecrets() ? new String(resolvePassword(value)) : "***";
         writer.writeAttribute(name, credential);
      } else if (value instanceof CredentialStoresConfigurationBuilder.CredentialStoreSourceSupplier) {
         CredentialStoresConfigurationBuilder.CredentialStoreSourceSupplier credentialSupplier = (CredentialStoresConfigurationBuilder.CredentialStoreSourceSupplier) value;
         writer.writeStartElement(Element.CREDENTIAL_REFERENCE);
         writer.writeAttribute(Attribute.STORE, credentialSupplier.getStore());
         writer.writeAttribute(Attribute.ALIAS, credentialSupplier.getAlias());
         writer.writeEndElement();
      } else {
         throw new IllegalArgumentException();
      }
   };
}
