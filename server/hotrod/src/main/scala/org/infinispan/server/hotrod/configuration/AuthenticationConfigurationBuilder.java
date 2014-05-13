package org.infinispan.server.hotrod.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.security.SaslUtils;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.hotrod.logging.JavaLog;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfigurationBuilder extends AbstractHotRodServerChildConfigurationBuilder implements Builder<AuthenticationConfiguration> {
   private static final JavaLog log = LogFactory.getLog(AuthenticationConfigurationBuilder.class, JavaLog.class);
   private boolean enabled = false;
   private ServerAuthenticationProvider serverAuthenticationProvider;
   private Set<String> allowedMechs = new LinkedHashSet<String>();
   private Map<String, String> mechProperties = new HashMap<String, String>();
   private String serverName;
   private Subject serverSubject;

   AuthenticationConfigurationBuilder(HotRodServerChildConfigurationBuilder builder) {
      super(builder);
   }

   public AuthenticationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public AuthenticationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public AuthenticationConfigurationBuilder serverAuthenticationProvider(ServerAuthenticationProvider serverAuthenticationProvider) {
      this.serverAuthenticationProvider = serverAuthenticationProvider;
      return this;
   }

   public AuthenticationConfigurationBuilder addAllowedMech(String mech) {
      this.allowedMechs.add(mech);
      return this;
   }

   public AuthenticationConfigurationBuilder mechProperties(Map<String, String> mechProperties) {
      this.mechProperties = mechProperties;
      return this;
   }

   public AuthenticationConfigurationBuilder addMechProperty(String key, String value) {
      this.mechProperties.put(key, value);
      return this;
   }

   public AuthenticationConfigurationBuilder serverName(String serverName) {
      this.serverName = serverName;
      return this;
   }

   public AuthenticationConfigurationBuilder serverSubject(Subject serverSubject) {
      this.serverSubject = serverSubject;
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (serverAuthenticationProvider == null) {
            throw log.serverAuthenticationProvider();
         }
         Set<String> allMechs = new LinkedHashSet<String>();
         for (Iterator<SaslServerFactory> factories = SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), true); factories.hasNext(); ) {
            SaslServerFactory factory = factories.next();
            for(String mech : factory.getMechanismNames(mechProperties)) {
               allMechs.add(mech);
            }
         }
         if (allowedMechs.isEmpty()) {
            allowedMechs = allMechs;
         } else if (!allMechs.containsAll(allowedMechs)){
            throw log.invalidAllowedMechs(allowedMechs, allMechs);
         }
         if (serverName == null) {
            throw log.missingServerName();
         }
      }
   }

   @Override
   public AuthenticationConfiguration create() {
      return new AuthenticationConfiguration(enabled, Collections.unmodifiableSet(allowedMechs), serverAuthenticationProvider, mechProperties, serverName, serverSubject);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template) {
      this.enabled = template.enabled();
      this.allowedMechs.clear();
      this.allowedMechs.addAll(template.allowedMechs());
      this.serverAuthenticationProvider = template.serverAuthenticationProvider();
      this.mechProperties = template.mechProperties();
      this.serverName = template.serverName();
      return this;
   }


}
