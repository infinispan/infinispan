/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.loaders.remote.configuration.as;

import static org.infinispan.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.as.OutboundSocketBinding;
import org.infinispan.configuration.as.ParserAS7;
import org.infinispan.configuration.as.ParserContextAS7;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserContext;
import org.infinispan.configuration.parsing.ParserContextListener;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfigurationBuilder;
import org.infinispan.loaders.remote.configuration.RemoteServerConfigurationBuilder;
import org.jboss.staxmapper.XMLExtendedStreamReader;

/**
 *
 * RemoteCacheStoreConfigurationParserAS7.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteCacheStoreConfigurationParserAS7 implements ConfigurationParser<ConfigurationBuilderHolder> {

   private static final Namespace NAMESPACES[] = {
      new Namespace(ParserAS7.URN_JBOSS_DOMAIN_INFINISPAN, Element.REMOTE_STORE.getLocalName(), 1, 4),
      new Namespace(ParserAS7.URN_JBOSS_DOMAIN_INFINISPAN, Element.REMOTE_STORE.getLocalName(), 1, 3),
   };

   public RemoteCacheStoreConfigurationParserAS7() {
   }

   @Override
   public Namespace[] getSupportedNamespaces() {
      return NAMESPACES;
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder) throws XMLStreamException {


      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case REMOTE_STORE: {
         parseRemoteStore(reader, holder);
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseRemoteStore(final XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ParserContextAS7 context = holder.getParserContext(ParserAS7.class);
      LoadersConfigurationBuilder loaders = holder.getCurrentConfigurationBuilder().loaders();
      RemoteCacheStoreConfigurationBuilder builder = new RemoteCacheStoreConfigurationBuilder(loaders);
      parseRemoteStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case REMOTE_SERVER: {
            parseServer(reader, builder.addServer(), context);
            break;
         }
         default: {
            ParserAS7.parseStoreElement(reader, builder);
         }
         }
      }
      loaders.addStore(builder);
   }

   private void parseServer(XMLExtendedStreamReader reader, final RemoteServerConfigurationBuilder builder, ParserContextAS7 context) throws XMLStreamException {
      final String value = ParseUtils.requireSingleAttribute(reader, Attribute.OUTBOUND_SOCKET_BINDING.getLocalName());
      context.addParsingCompleteListener(new ParserContextListener() {

         @Override
         public void parsingComplete(ParserContext context) {
            ParserContextAS7 ctx = (ParserContextAS7) context;
            OutboundSocketBinding binding = ctx.getOutboundSocketBinding(value);
            builder.host(binding.host()).port(binding.port());
         }
      });
      ParseUtils.requireNoContent(reader);
   }

   private void parseRemoteStoreAttributes(XMLExtendedStreamReader reader, RemoteCacheStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case CACHE: {
            builder.remoteCacheName(value);
            break;
         }
         case SOCKET_TIMEOUT: {
            builder.socketTimeout(Long.parseLong(value));
            break;
         }
         case TCP_NO_DELAY: {
            builder.tcpNoDelay(Boolean.parseBoolean(value));
            break;
         }
         default: {
            ParserAS7.parseStoreAttribute(reader, i, builder);
            break;
         }
         }
      }
   }
}
