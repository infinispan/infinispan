/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * The parser for the data grid endpoint subsystem configuration.
 *
 * @author Tristan Tarrant
 *
 */
class EndpointSubsystemReader_8_0 implements XMLStreamConstants, XMLElementReader<List<ModelNode>> {
   public static final XMLElementReader<List<ModelNode>> INSTANCE = new EndpointSubsystemReader_8_0();

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final List<ModelNode> operations)
         throws XMLStreamException {

      PathAddress subsystemAddress = PathAddress.pathAddress(Constants.SUBSYSTEM_PATH);
      ModelNode subsystem = Util.createAddOperation(subsystemAddress);

      operations.add(subsystem);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case HOTROD_CONNECTOR: {
            parseHotRodConnector(reader, subsystemAddress, operations);
            break;
         }
         case MEMCACHED_CONNECTOR: {
            parseMemcachedConnector(reader, subsystemAddress, operations);
            break;
         }
         case REST_CONNECTOR: {
            parseRestConnector(reader, subsystemAddress, operations);
            break;
         }
         case WEBSOCKET_CONNECTOR: {
            parseWebSocketConnector(reader, subsystemAddress, operations);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseHotRodConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress,
         List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.HOTROD_CONNECTOR;
      final Set<Attribute> required = EnumSet.of(Attribute.SOCKET_BINDING);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         required.remove(attribute);
         name = parseConnectorAttributes(reader, connector, name, i, value, attribute);
      }

      if (!required.isEmpty()) {
         throw ParseUtils.missingRequired(reader, required);
      }

      PathAddress connectorAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.HOTROD_CONNECTOR, name));
      connector.get(OP_ADDR).set(connectorAddress.toModelNode());

      operations.add(connector);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case TOPOLOGY_STATE_TRANSFER: {
            parseTopologyStateTransfer(reader, connector, operations);
            break;
         }
         case AUTHENTICATION: {
            parseAuthentication(reader, connector, operations);
            break;
         }
         case ENCRYPTION: {
            parseEncryption(reader, connector, operations);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseMemcachedConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress,
         List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.MEMCACHED_CONNECTOR;
      final Set<Attribute> required = EnumSet.of(Attribute.SOCKET_BINDING);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         required.remove(attribute);
         switch (attribute) {
         case CACHE:
            MemcachedConnectorResource.CACHE.parseAndSetParameter(value, connector, reader);
            break;
         default:
            name = parseConnectorAttributes(reader, connector, name, i, value, attribute);
            break;
         }
      }

      if (!required.isEmpty()) {
         throw ParseUtils.missingRequired(reader, required);
      }

      PathAddress connectorAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.MEMCACHED_CONNECTOR,
            name));
      connector.get(OP_ADDR).set(connectorAddress.toModelNode());

      ParseUtils.requireNoContent(reader);

      operations.add(connector);
   }

   private String parseConnectorAttributes(XMLExtendedStreamReader reader, ModelNode connector, String name, int i,
         String value, Attribute attribute) throws XMLStreamException {
      switch (attribute) {
      case CACHE_CONTAINER: {
         CommonConnectorResource.CACHE_CONTAINER.parseAndSetParameter(value, connector, reader);
         break;
      }
      case IDLE_TIMEOUT: {
         ProtocolServerConnectorResource.IDLE_TIMEOUT.parseAndSetParameter(value, connector, reader);
         break;
      }
      case NAME: {
         CommonConnectorResource.NAME.parseAndSetParameter(value, connector, reader);
         name = value;
         break;
      }
      case RECEIVE_BUFFER_SIZE: {
         ProtocolServerConnectorResource.RECEIVE_BUFFER_SIZE.parseAndSetParameter(value, connector, reader);
         break;
      }
      case SEND_BUFFER_SIZE: {
         ProtocolServerConnectorResource.SEND_BUFFER_SIZE.parseAndSetParameter(value, connector, reader);
         break;
      }
      case SOCKET_BINDING: {
         ProtocolServerConnectorResource.SOCKET_BINDING.parseAndSetParameter(value, connector, reader);
         break;
      }
      case TCP_NODELAY: {
         ProtocolServerConnectorResource.TCP_NODELAY.parseAndSetParameter(value, connector, reader);
         break;
      }
      case WORKER_THREADS: {
         ProtocolServerConnectorResource.WORKER_THREADS.parseAndSetParameter(value, connector, reader);
         break;
      }
      default: {
         throw ParseUtils.unexpectedAttribute(reader, i);
      }
      }
      return name;
   }

   private void parseRestConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress,
         List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.REST_CONNECTOR;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case SOCKET_BINDING: {
            RestConnectorResource.SOCKET_BINDING.parseAndSetParameter(value, connector, reader);
            break;
         }
         case AUTH_METHOD: {
            RestConnectorResource.AUTH_METHOD.parseAndSetParameter(value, connector, reader);
            break;
         }
         case CACHE_CONTAINER: {
            CommonConnectorResource.CACHE_CONTAINER.parseAndSetParameter(value, connector, reader);
            break;
         }
         case CONTEXT_PATH: {
            RestConnectorResource.CONTEXT_PATH.parseAndSetParameter(value, connector, reader);
            break;
         }
         case EXTENDED_HEADERS: {
            RestConnectorResource.EXTENDED_HEADERS.parseAndSetParameter(value, connector, reader);
            break;
         }
         case NAME: {
            CommonConnectorResource.NAME.parseAndSetParameter(value, connector, reader);
            name = value;
            break;
         }
         case SECURITY_DOMAIN: {
            RestConnectorResource.SECURITY_DOMAIN.parseAndSetParameter(value, connector, reader);
            break;
         }
         case SECURITY_MODE: {
            RestConnectorResource.SECURITY_MODE.parseAndSetParameter(value, connector, reader);
            break;
         }
         case VIRTUAL_HOST: {
            ROOT_LOGGER.virtualHostNotInUse();
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }

      PathAddress containerAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.REST_CONNECTOR, name));
      connector.get(OP_ADDR).set(containerAddress.toModelNode());

      ParseUtils.requireNoContent(reader);

      operations.add(connector);
   }

   private void parseWebSocketConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress,
         List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.WEBSOCKET_CONNECTOR;
      final Set<Attribute> required = EnumSet.of(Attribute.SOCKET_BINDING);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         required.remove(attribute);
         name = parseConnectorAttributes(reader, connector, name, i, value, attribute);
      }

      if (!required.isEmpty()) {
         throw ParseUtils.missingRequired(reader, required);
      }

      PathAddress connectorAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.WEBSOCKET_CONNECTOR,
            name));
      connector.get(OP_ADDR).set(connectorAddress.toModelNode());

      ParseUtils.requireNoContent(reader);

      operations.add(connector);
   }

   private void parseTopologyStateTransfer(XMLExtendedStreamReader reader, ModelNode connector,
         List<ModelNode> operations) throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(connector.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME));
      ModelNode topologyStateTransfer = Util.createAddOperation(address);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case AWAIT_INITIAL_RETRIEVAL: {
            TopologyStateTransferResource.AWAIT_INITIAL_RETRIEVAL.parseAndSetParameter(value, topologyStateTransfer,
                  reader);
            break;
         }
         case EXTERNAL_HOST: {
            TopologyStateTransferResource.EXTERNAL_HOST.parseAndSetParameter(value, topologyStateTransfer, reader);
            break;
         }
         case EXTERNAL_PORT: {
            TopologyStateTransferResource.EXTERNAL_PORT.parseAndSetParameter(value, topologyStateTransfer, reader);
            break;
         }
         case LAZY_RETRIEVAL: {
            TopologyStateTransferResource.LAZY_RETRIEVAL.parseAndSetParameter(value, topologyStateTransfer, reader);
            break;
         }
         case LOCK_TIMEOUT: {
            TopologyStateTransferResource.LOCK_TIMEOUT.parseAndSetParameter(value, topologyStateTransfer, reader);
            break;
         }
         case REPLICATION_TIMEOUT: {
            TopologyStateTransferResource.REPLICATION_TIMEOUT
                  .parseAndSetParameter(value, topologyStateTransfer, reader);
            break;
         }
         case UPDATE_TIMEOUT: {
            ROOT_LOGGER.topologyUpdateTimeoutIgnored();
            break;
         }
         default: {
            ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
      operations.add(topologyStateTransfer);

   }

   private void parseAuthentication(XMLExtendedStreamReader reader, ModelNode connector, List<ModelNode> operations)
         throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(connector.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.AUTHENTICATION, ModelKeys.AUTHENTICATION_NAME));
      ModelNode authentication = Util.createAddOperation(address);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case SECURITY_REALM: {
            AuthenticationResource.SECURITY_REALM.parseAndSetParameter(value, authentication, reader);
            break;
         }
         default: {
            ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      operations.add(authentication);

      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         if (visited.contains(element)) {
            throw ParseUtils.unexpectedElement(reader);
         }
         visited.add(element);
         switch (element) {
         case SASL: {
            parseSasl(reader, authentication, operations);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseSasl(final XMLExtendedStreamReader reader, final ModelNode authentication, final List<ModelNode> list) throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(authentication.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.SASL, ModelKeys.SASL_NAME));
      ModelNode sasl = Util.createAddOperation(address);
      list.add(sasl);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case MECHANISMS: {
            for(String mech : reader.getListAttributeValue(i)) {
               SaslResource.MECHANISMS.parseAndAddParameterElement(mech, sasl, reader);
            }
            break;
         }
         case QOP: {
            for(String qop : reader.getListAttributeValue(i)) {
               SaslResource.QOP.parseAndAddParameterElement(qop, sasl, reader);
            }
            break;
         }
         case SERVER_CONTEXT_NAME: {
            SaslResource.SERVER_CONTEXT_NAME.parseAndSetParameter(value, sasl, reader);
            break;
         }
         case SERVER_NAME: {
            SaslResource.SERVER_NAME.parseAndSetParameter(value, sasl, reader);
            break;
         }
         case STRENGTH: {
            for(String strength : reader.getListAttributeValue(i)) {
               SaslResource.STRENGTH.parseAndAddParameterElement(strength, sasl, reader);
            }
            break;
         }
         default: {
            ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      // Nested elements
      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case POLICY: {
            if (visited.contains(element)) {
               throw ParseUtils.unexpectedElement(reader);
            } else {
               visited.add(element);
            }
            parsePolicy(reader, sasl, list);
            break;
         }
         case PROPERTY: {
            parseProperty(reader, sasl, list);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   void parsePolicy(XMLExtendedStreamReader reader, final ModelNode sasl, final List<ModelNode> list)
         throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(sasl.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.SASL_POLICY, ModelKeys.SASL_POLICY_NAME));
      ModelNode policy = Util.createAddOperation(address);
      list.add(policy);

      if (reader.getAttributeCount() > 0) {
         throw ParseUtils.unexpectedAttribute(reader, 0);
      }
      // Handle nested elements.
      final EnumSet<Element> visited = EnumSet.noneOf(Element.class);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         final Element element = Element.forName(reader.getLocalName());
         if (visited.contains(element)) {
            throw ParseUtils.unexpectedElement(reader);
         }
         visited.add(element);
         switch (element) {
         case FORWARD_SECRECY: {
            SaslPolicyResource.FORWARD_SECRECY.parseAndSetParameter(
                  ParseUtils.readStringAttributeElement(reader, "value"), policy, reader);
            break;
         }
         case NO_ACTIVE: {
            SaslPolicyResource.NO_ACTIVE.parseAndSetParameter(ParseUtils.readStringAttributeElement(reader, "value"),
                  policy, reader);
            break;
         }
         case NO_ANONYMOUS: {
            SaslPolicyResource.NO_ANONYMOUS.parseAndSetParameter(
                  ParseUtils.readStringAttributeElement(reader, "value"), policy, reader);
            break;
         }
         case NO_DICTIONARY: {
            SaslPolicyResource.NO_DICTIONARY.parseAndSetParameter(
                  ParseUtils.readStringAttributeElement(reader, "value"), policy, reader);
            break;
         }
         case NO_PLAIN_TEXT: {
            SaslPolicyResource.NO_PLAIN_TEXT.parseAndSetParameter(
                  ParseUtils.readStringAttributeElement(reader, "value"), policy, reader);
            break;
         }
         case PASS_CREDENTIALS: {
            SaslPolicyResource.PASS_CREDENTIALS.parseAndSetParameter(
                  ParseUtils.readStringAttributeElement(reader, "value"), policy, reader);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseProperty(XMLExtendedStreamReader reader, ModelNode node, final List<ModelNode> operations) throws XMLStreamException {
      ParseUtils.requireSingleAttribute(reader, Attribute.NAME.getLocalName());
      String propertyName = reader.getAttributeValue(0);
      String propertyValue = reader.getElementText();

      PathAddress propertyAddress = PathAddress.pathAddress(node.get(OP_ADDR)).append(ModelKeys.PROPERTY, propertyName);
      ModelNode property = Util.createAddOperation(propertyAddress);

      // represent the value as a ModelNode to cater for expressions
      SaslPropertyResource.VALUE.parseAndSetParameter(propertyValue, property, reader);

      operations.add(property);
  }

   private void parseEncryption(XMLExtendedStreamReader reader, ModelNode connector, List<ModelNode> operations)
         throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(connector.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME));
      ModelNode security = Util.createAddOperation(address);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case REQUIRE_SSL_CLIENT_AUTH: {
            EncryptionResource.REQUIRE_SSL_CLIENT_AUTH.parseAndSetParameter(value, security, reader);
            break;
         }
         case SECURITY_REALM: {
            EncryptionResource.SECURITY_REALM.parseAndSetParameter(value, security, reader);
            break;
         }
         default: {
            ParseUtils.unexpectedAttribute(reader, i);
         }
         }

      }
      ParseUtils.requireNoContent(reader);
      operations.add(security);
   }
}
