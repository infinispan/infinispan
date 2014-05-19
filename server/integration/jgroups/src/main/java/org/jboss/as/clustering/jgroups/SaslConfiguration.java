package org.jboss.as.clustering.jgroups;

import org.jboss.as.domain.management.SecurityRealm;

/**
 * SaslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface SaslConfiguration extends ProtocolConfiguration {
   String getClusterRole();

   SecurityRealm getSecurityRealm();

   String getMech();
}
