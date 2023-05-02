package org.infinispan.server.configuration.security;

import java.util.EnumSet;
import java.util.Properties;

import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public interface RealmProvider {
   SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties);

   String name();

   void applyFeatures(EnumSet<ServerSecurityRealm.Feature> features);
}
