package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.ServerConfigurationParser.NAMESPACE;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.SerializeUtils;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.configuration.endpoint.EndpointConfiguration;
import org.infinispan.server.configuration.endpoint.EndpointsConfiguration;
import org.infinispan.server.configuration.security.CredentialStoreConfiguration;
import org.infinispan.server.configuration.security.CredentialStoresConfiguration;
import org.infinispan.server.configuration.security.FileSystemRealmConfiguration;
import org.infinispan.server.configuration.security.KerberosSecurityFactoryConfiguration;
import org.infinispan.server.configuration.security.LdapAttributeConfiguration;
import org.infinispan.server.configuration.security.LdapAttributeMappingConfiguration;
import org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration;
import org.infinispan.server.configuration.security.LdapRealmConfiguration;
import org.infinispan.server.configuration.security.LdapUserPasswordMapperConfiguration;
import org.infinispan.server.configuration.security.LocalRealmConfiguration;
import org.infinispan.server.configuration.security.PropertiesRealmConfiguration;
import org.infinispan.server.configuration.security.RealmConfiguration;
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
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.util.RegexNameRewriter;

public class ServerConfigurationSerializer
      implements ConfigurationSerializer<ServerConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, ServerConfiguration configuration) {
      writer.writeStartDocument();
      writer.writeStartElement(Element.SERVER);
      writer.writeDefaultNamespace(NAMESPACE + Version.getMajorMinor());
      writeInterfaces(writer, configuration.interfaces);
      writeSocketBindings(writer, configuration.socketBindings);
      writeSecurity(writer, configuration.security());
      writeDataSources(writer, configuration.dataSources());
      writeEndpoints(writer, configuration.endpoints());
      writer.writeEndElement();
      writer.writeEndDocument();
   }

   private void writeInterfaces(ConfigurationWriter writer, InterfacesConfiguration networkInterfaces) {
      writer.writeStartListElement(Element.INTERFACES, true);
      for (InterfaceConfiguration netIf : networkInterfaces.interfaces()) {
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
      writer.writeStartListElement(Element.SOCKET_BINDINGS, true);
      socketBindings.attributes().write(writer);
      if (writer.hasFeature(ConfigurationFormatFeature.MIXED_ELEMENTS)) {
         for (SocketBindingConfiguration socketBinding : socketBindings.socketBindings()) {
            writer.writeStartElement(Element.SOCKET_BINDING);
            socketBinding.attributes().write(writer);
            writer.writeEndElement(); // SOCKET_BINDING
         }
      } else {
         writer.writeStartElement(Element.SOCKET_BINDING);
         for (SocketBindingConfiguration socketBinding : socketBindings.socketBindings()) {
            writer.writeStartArrayElement(Element.SOCKET_BINDING);
            socketBinding.attributes().write(writer);
            writer.writeEndArrayElement(); // SOCKET_BINDING
         }
         writer.writeEndElement();
      }
      writer.writeEndListElement(); // SOCKET_BINDINGS
   }

   private void writeSecurity(ConfigurationWriter writer, SecurityConfiguration security) {
      writer.writeStartElement(Element.SECURITY);
      writeCredentialStores(writer, security.credentialStores());
      writeSecurityRealms(writer, security.realms());
      writer.writeEndElement();
   }

   private void writeCredentialStores(ConfigurationWriter writer, CredentialStoresConfiguration credentialStores) {
      writer.writeStartArrayElement(Element.CREDENTIAL_STORES);
      for (CredentialStoreConfiguration credentialStore : credentialStores.credentialStores()) {
         credentialStore.write(writer);
      }
      writer.writeEndArrayElement();
   }

   private void writeSecurityRealms(ConfigurationWriter writer, RealmsConfiguration realms) {
      writer.writeStartArrayElement(Element.SECURITY_REALMS);
      for (RealmConfiguration realm : realms.realms()) {
         writer.writeStartElement(Element.SECURITY_REALM);
         realm.attributes().write(writer);
         writeServerIdentities(writer, realm.serverIdentitiesConfiguration());
         writeRealm(writer, realm.fileSystemConfiguration());
         writeRealm(writer, realm.ldapConfiguration());
         writeRealm(writer, realm.localConfiguration());
         writeRealm(writer, realm.propertiesRealm());
         writeRealm(writer, realm.tokenConfiguration());
         writeRealm(writer, realm.trustStoreConfiguration());
         writer.writeEndElement(); // SECURITY_REALM
      }
      writer.writeEndArrayElement(); // SECURITY_REALMS
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
         for (LdapIdentityMappingConfiguration identity : realm.identityMappings()) {
            writer.writeStartElement(Element.IDENTITY_MAPPING);
            identity.attributes().write(writer);
            if (!identity.attributeMappings().isEmpty()) {
               writer.writeStartElement(Element.ATTRIBUTE_MAPPING);
               for (LdapAttributeMappingConfiguration mapping : identity.attributeMappings()) {
                  for (LdapAttributeConfiguration attribute : mapping.attributesConfiguration()) {
                     attribute.write(writer);
                  }
               }
               writer.writeEndElement(); // ATTRIBUTE_MAPPING
            }
            if (!identity.userPasswordMapper().isEmpty()) {
               for (LdapUserPasswordMapperConfiguration mapper : identity.userPasswordMapper()) {
                  mapper.attributes().write(writer, Element.USER_PASSWORD_MAPPER);
               }
            }
            writer.writeEndElement(); // IDENTITY_MAPPING
         }
         writer.writeEndElement(); // LDAP_REALM
      }
   }

   private void writeRealm(ConfigurationWriter writer, LocalRealmConfiguration realm) {
      realm.attributes().write(writer, Element.LOCAL_REALM);
   }

   private void writeRealm(ConfigurationWriter writer, FileSystemRealmConfiguration realm) {
      realm.write(writer);
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

   private void writeServerIdentities(ConfigurationWriter writer, ServerIdentitiesConfiguration identities) {
      writer.writeStartElement(Element.SERVER_IDENTITIES);
      SSLConfiguration ssl = identities.sslConfiguration();
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
      List<KerberosSecurityFactoryConfiguration> kerberosList = identities.kerberosConfigurations();
      if (!kerberosList.isEmpty()) {
         for (KerberosSecurityFactoryConfiguration kerberos : kerberosList) {
            kerberos.write(writer);
         }
      }
      writer.writeEndElement();
   }

   private void writeDataSources(ConfigurationWriter writer, Map<String, DataSourceConfiguration> dataSources) {
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
         // We don't serialize the password
         attributes.write(writer, DataSourceConfiguration.URL);
         attributes.write(writer, DataSourceConfiguration.INITIAL_SQL);
         Map<String, String> properties = attributes.attribute(DataSourceConfiguration.CONNECTION_PROPERTIES).get();
         SerializeUtils.writeTypedProperties(writer, TypedProperties.toTypedProperties(properties), Element.CONNECTION_PROPERTIES, Element.CONNECTION_PROPERTY, false);
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

   private void writeEndpoints(ConfigurationWriter writer, EndpointsConfiguration endpoints) {
      for (EndpointConfiguration endpoint : endpoints.endpoints()) {
         writer.writeStartElement(Element.ENDPOINTS);
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
   }

   private void writeConnector(ConfigurationWriter writer, HotRodServerConfiguration connector) {
      writer.writeStartElement(org.infinispan.server.hotrod.configuration.Element.HOTROD_CONNECTOR);
      connector.attributes().write(writer);
      connector.topologyCache().write(writer);
      if (connector.authentication().enabled()) {
         writer.writeStartElement(org.infinispan.server.hotrod.configuration.Element.AUTHENTICATION);
         connector.authentication().attributes().write(writer);
         connector.authentication().sasl().write(writer);
         SerializeUtils.writeTypedProperties(writer, TypedProperties.toTypedProperties(connector.authentication().mechProperties()), Element.PROPERTIES, org.infinispan.server.hotrod.configuration.Element.PROPERTY, false);
         writer.writeEndElement();
      }
      connector.encryption().write(writer);
      writer.writeEndElement();
   }

   private void writeConnector(ConfigurationWriter writer, RestServerConfiguration connector) {
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
      connector.write(writer);
   }
}
