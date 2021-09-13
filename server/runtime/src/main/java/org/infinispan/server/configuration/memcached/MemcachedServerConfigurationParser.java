package org.infinispan.server.configuration.memcached;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.configuration.ServerConfigurationBuilder;
import org.infinispan.server.configuration.ServerConfigurationParser;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.kohsuke.MetaInfServices;

/**
 * Server endpoint configuration parser for memcached
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "memcached-connector"),
      @Namespace(uri = "urn:infinispan:server:*", root = "memcached-connector"),
})
public class MemcachedServerConfigurationParser implements ConfigurationParser {
   private static org.infinispan.util.logging.Log coreLog = org.infinispan.util.logging.LogFactory.getLog(ServerConfigurationParser.class);

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder)
         {
      if (!holder.inScope(ServerConfigurationParser.ENDPOINTS_SCOPE)) {
         throw coreLog.invalidScope(ServerConfigurationParser.ENDPOINTS_SCOPE, holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case MEMCACHED_CONNECTOR: {
            ServerConfigurationBuilder serverBuilder = builder.module(ServerConfigurationBuilder.class);
            if (serverBuilder != null) {
               parseMemcached(reader, serverBuilder, serverBuilder.endpoints().current().addConnector(MemcachedServerConfigurationBuilder.class));
            } else {
               throw ParseUtils.unexpectedElement(reader);
            }
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseMemcached(ConfigurationReader reader, ServerConfigurationBuilder serverBuilder, MemcachedServerConfigurationBuilder builder) {
      String[] required = ParseUtils.requireAttributes(reader, Attribute.SOCKET_BINDING);
      builder.startTransport(true);
      builder.socketBinding(required[0]);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CACHE: {
               builder.cache(value);
               break;
            }
            case CACHE_CONTAINER: {
               break;
            }
            case CLIENT_ENCODING: {
               builder.clientEncoding(MediaType.fromString(value));
               break;
            }
            case IDLE_TIMEOUT: {
               builder.idleTimeout(Integer.parseInt(value));
               break;
            }
            case IO_THREADS: {
               builder.ioThreads(Integer.parseInt(value));
               break;
            }
            case NAME: {
               builder.name(value);
               break;
            }
            case SOCKET_BINDING:
               // already seen
               break;
            default: {
               ServerConfigurationParser.parseCommonConnectorAttributes(reader, i, serverBuilder, builder);
            }
         }
      }
      while (reader.inTag()) {
         ServerConfigurationParser.parseCommonConnectorElements(reader, builder);
      }
   }
}
