package org.infinispan.server.resp.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.EncryptionConfiguration;
import org.infinispan.server.core.configuration.IpFilterConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.resp.RespServer;

/**
 * RespServerConfiguration.
 *
 * @author William Burns
 * @since 14.0
 */
@BuiltBy(RespServerConfigurationBuilder.class)
@ConfigurationFor(RespServer.class)
public class RespServerConfiguration extends ProtocolServerConfiguration<RespServerConfiguration> {

   public static final int DEFAULT_RESP_PORT = 6379;
   public static final String DEFAULT_RESP_CACHE = "respCache";

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RespServerConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet());
   }

   private final AuthenticationConfiguration authentication;
   private final EncryptionConfiguration encryption;

   RespServerConfiguration(AttributeSet attributes, IpFilterConfiguration ipRules, SslConfiguration ssl,
                           AuthenticationConfiguration authentication, EncryptionConfiguration encryption) {
      super("resp-connector", attributes, ssl, ipRules);
      this.authentication = authentication;
      this.encryption = encryption;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public EncryptionConfiguration encryption() {
      return encryption;
   }
}
