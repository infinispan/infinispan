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

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.controller.parsing.ParseUtils;
import org.jboss.dmr.ModelNode;
import org.jboss.staxmapper.XMLElementReader;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 * The parser for the data grid endpoint subsystem configuration.
 *
 * @author Tristan Tarrant
 *
 */
class EndpointSubsystemReader_6_0 implements XMLStreamConstants, XMLElementReader<List<ModelNode>> {

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final List<ModelNode> operations) throws XMLStreamException {

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

   private void parseHotRodConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress, List<ModelNode> operations) throws XMLStreamException {

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
         case SECURITY: {
            parseSecurity(reader, connector, operations);
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
         }
      }
   }

   private void parseMemcachedConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress, List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.MEMCACHED_CONNECTOR;
      final Set<Attribute> required = EnumSet.of(Attribute.SOCKET_BINDING);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         required.remove(attribute);
         switch(attribute) {
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

      PathAddress connectorAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.MEMCACHED_CONNECTOR, name));
      connector.get(OP_ADDR).set(connectorAddress.toModelNode());

      ParseUtils.requireNoContent(reader);

      operations.add(connector);
   }

   private String parseConnectorAttributes(XMLExtendedStreamReader reader, ModelNode connector, String name, int i, String value, Attribute attribute) throws XMLStreamException {
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

   private void parseRestConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress, List<ModelNode> operations) throws XMLStreamException {

      ModelNode connector = Util.getEmptyOperation(ADD, null);
      String name = ModelKeys.REST_CONNECTOR;

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
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
         case VIRTUAL_SERVER: {
            RestConnectorResource.VIRTUAL_SERVER.parseAndSetParameter(value, connector, reader);
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

   private void parseWebSocketConnector(XMLExtendedStreamReader reader, PathAddress subsystemAddress, List<ModelNode> operations) throws XMLStreamException {

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

      PathAddress connectorAddress = subsystemAddress.append(PathElement.pathElement(ModelKeys.WEBSOCKET_CONNECTOR, name));
      connector.get(OP_ADDR).set(connectorAddress.toModelNode());

      ParseUtils.requireNoContent(reader);

      operations.add(connector);
   }

   private void parseTopologyStateTransfer(XMLExtendedStreamReader reader, ModelNode connector, List<ModelNode> operations) throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(connector.get(OP_ADDR)).append(
            PathElement.pathElement(ModelKeys.TOPOLOGY_STATE_TRANSFER, ModelKeys.TOPOLOGY_STATE_TRANSFER_NAME));
      ModelNode topologyStateTransfer = Util.createAddOperation(address);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case AWAIT_INITIAL_RETRIEVAL: {
            TopologyStateTransferResource.AWAIT_INITIAL_RETRIEVAL.parseAndSetParameter(value, topologyStateTransfer, reader);
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
            TopologyStateTransferResource.REPLICATION_TIMEOUT.parseAndSetParameter(value, topologyStateTransfer, reader);
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

   private void parseSecurity(XMLExtendedStreamReader reader, ModelNode connector, List<ModelNode> operations) throws XMLStreamException {
      PathAddress address = PathAddress.pathAddress(connector.get(OP_ADDR)).append(PathElement.pathElement(ModelKeys.SECURITY, ModelKeys.SECURITY_NAME));
      ModelNode security = Util.createAddOperation(address);

      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case REQUIRE_CLIENT_AUTH: {
            SecurityResource.REQUIRE_SSL_CLIENT_AUTH.parseAndSetParameter(value, security, reader);
            break;
         }
         case SECURITY_REALM: {
            SecurityResource.SECURITY_REALM.parseAndSetParameter(value, security, reader);
            break;
         }
         case SSL: {
            SecurityResource.SSL.parseAndSetParameter(value, security, reader);
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
