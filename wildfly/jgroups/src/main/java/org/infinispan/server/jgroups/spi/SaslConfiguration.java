package org.infinispan.server.jgroups.spi;

import org.jboss.as.domain.management.SecurityRealm;

/**
 * SaslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface SaslConfiguration extends ProtocolConfiguration {
    String PROTOCOL_NAME = "SASL";

    String getClusterRole();

    SecurityRealm getSecurityRealm();

    String getMech();
}
