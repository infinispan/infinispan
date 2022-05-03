package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.ServerConfiguration.HOST;
import static org.infinispan.hotrod.configuration.ServerConfiguration.PORT;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * ServerConfigurationBuilder.
 *
 * @since 14.0
 */
public class ServerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ServerConfiguration> {
   private static final Pattern ADDRESS_PATTERN = Pattern.compile("(\\[([0-9A-Fa-f:]+)\\]|([^:/?#]*))(?::(\\d*))?");
   private final AttributeSet attributes = ServerConfiguration.attributeDefinitionSet();

   ServerConfigurationBuilder(HotRodConfigurationBuilder builder) {
      super(builder);
   }

   public ServerConfigurationBuilder host(String host) {
      attributes.attribute(HOST).set(host);
      return this;
   }

   public ServerConfigurationBuilder port(int port) {
      attributes.attribute(PORT).set(port);
      return this;
   }

   @Override
   public void validate() {

   }

   @Override
   public ServerConfiguration create() {
      return new ServerConfiguration(attributes.protect());
   }

   @Override
   public Builder read(ServerConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   public static void parseServers(String servers, BiConsumer<String, Integer> c) {
      for (String server : servers.split(";")) {
         Matcher matcher = ADDRESS_PATTERN.matcher(server.trim());
         if (matcher.matches()) {
            String v6host = matcher.group(2);
            String v4host = matcher.group(3);
            String host = v6host != null ? v6host : v4host;
            String portString = matcher.group(4);
            int port = portString == null
                  ? PORT.getDefaultValue()
                  : Integer.parseInt(portString);
            c.accept(host, port);
         } else {
            throw HOTROD.parseErrorServerAddress(server);
         }

      }
   }

}
