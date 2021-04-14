package org.infinispan.persistence.remote.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.SerializeUtils;

/**
 * RemoteStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class RemoteStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<RemoteStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, RemoteStoreConfiguration configuration) {
      writer.writeStartElement(Element.REMOTE_STORE);
      writer.writeDefaultNamespace(RemoteStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);

      writeCommonStoreSubAttributes(writer, configuration);

      writeAsyncExecutor(writer, configuration.asyncExecutorFactory());
      writeConnectionPool(writer, configuration.connectionPool());
      writeServers(writer, configuration.servers());
      writeSecurity(writer, configuration.security());

      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeAsyncExecutor(ConfigurationWriter writer, ExecutorFactoryConfiguration executorFactoryConfiguration) {
      AttributeSet attributes = executorFactoryConfiguration.attributes();
      if(attributes.isModified()) {
         writer.writeStartElement(Element.ASYNC_TRANSPORT_EXECUTOR);
         attributes.write(writer, ExecutorFactoryConfiguration.EXECUTOR_FACTORY, Attribute.FACTORY);
         SerializeUtils.writeTypedProperties(writer, executorFactoryConfiguration.properties());
         writer.writeEndElement();
      }
   }

   private void writeConnectionPool(ConfigurationWriter writer, ConnectionPoolConfiguration connectionPool) {
      writer.writeStartElement(Element.CONNECTION_POOL);
      writer.writeAttribute(Attribute.EXHAUSTED_ACTION, connectionPool.exhaustedAction().name());
      writer.writeAttribute(Attribute.MAX_ACTIVE, Integer.toString(connectionPool.maxActive()));
      writer.writeAttribute(Attribute.MAX_PENDING_REQUESTS, Integer.toString(connectionPool.maxPendingRequests()));
      writer.writeAttribute(Attribute.MAX_WAIT, Integer.toString(connectionPool.maxWait()));
      writer.writeAttribute(Attribute.MIN_IDLE, Integer.toString(connectionPool.minIdle()));
      writer.writeAttribute(Attribute.MIN_EVICTABLE_IDLE_TIME, Long.toString(connectionPool.minEvictableIdleTime()));
      writer.writeEndElement();
   }

   private void writeServers(ConfigurationWriter writer, List<RemoteServerConfiguration> servers) {
      writer.writeStartListElement(Element.REMOTE_SERVER, false);
      for(RemoteServerConfiguration server : servers) {
         writer.writeStartElement(Element.REMOTE_SERVER);
         writer.writeAttribute(Attribute.HOST, server.host());
         writer.writeAttribute(Attribute.PORT, Integer.toString(server.port()));
         writer.writeEndElement();
      }
      writer.writeEndListElement();
   }

   private void writeSecurity(ConfigurationWriter writer, SecurityConfiguration security) {
      if (security.authentication().attributes().isModified() || security.ssl().attributes().isModified()) {
         writer.writeStartElement(Element.SECURITY);
         writeAuthentication(writer, security.authentication());
         writeEncryption(writer, security.ssl());
         writer.writeEndElement();
      }
   }

   private void writeAuthentication(ConfigurationWriter writer, AuthenticationConfiguration authentication) {
      AttributeSet attributeSet = authentication.attributes();
      if (attributeSet.isModified()) {
         writer.writeStartElement(Element.AUTHENTICATION);
         attributeSet.write(writer);
         switch (authentication.saslMechanism()) {
            case "PLAIN": {
               writer.writeStartElement(Element.AUTH_PLAIN);
               writer.writeAttribute(Attribute.USERNAME, authentication.username());
               writer.writeAttribute(Attribute.PASSWORD, new String(authentication.password()));
               writer.writeEndElement();
               break;
            }
            case "DIGEST-MD5": {
               writer.writeStartElement(Element.AUTH_DIGEST);
               writer.writeAttribute(Attribute.USERNAME, authentication.username());
               writer.writeAttribute(Attribute.PASSWORD, new String(authentication.password()));
               writer.writeAttribute(Attribute.REALM, authentication.realm());
               writer.writeEndElement();
               break;
            }
            case "EXTERNAL": {
               writer.writeEmptyElement(Element.AUTH_EXTERNAL);
               break;
            }

         }
         writer.writeEndElement();
      }
   }

   private void writeEncryption(ConfigurationWriter writer, SslConfiguration ssl) {
      AttributeSet attributes = ssl.attributes();
      if (attributes.isModified()) {
         writer.writeStartElement(Element.ENCRYPTION);
         attributes.write(writer);
         if (ssl.keyStoreFileName() != null) {
            writer.writeStartElement(Element.KEYSTORE);
            writer.writeAttribute(Attribute.FILENAME, ssl.keyStoreFileName());
            writer.writeAttribute(Attribute.PASSWORD, new String(ssl.keyStorePassword()));
            writer.writeAttribute(Attribute.CERTIFICATE_PASSWORD, new String(ssl.keyStoreCertificatePassword()));
            writer.writeAttribute(Attribute.KEY_ALIAS, ssl.keyAlias());
            writer.writeAttribute(Attribute.TYPE, ssl.keyStoreType());
            writer.writeEndElement();
         }
         if (ssl.trustStoreFileName() != null) {
            writer.writeStartElement(Element.TRUSTSTORE);
            writer.writeAttribute(Attribute.FILENAME, ssl.trustStoreFileName());
            writer.writeAttribute(Attribute.PASSWORD, new String(ssl.trustStorePassword()));
            writer.writeAttribute(Attribute.TYPE, ssl.trustStoreType());
            writer.writeEndElement();
         }
         writer.writeEndElement();
      }
   }


}
