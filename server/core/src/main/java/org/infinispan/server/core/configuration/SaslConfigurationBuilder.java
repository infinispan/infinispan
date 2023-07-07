package org.infinispan.server.core.configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.SaslUtils;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.security.external.ExternalSaslServerFactory;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;

/**
 * @since 10.0
 */
public class SaslConfigurationBuilder implements Builder<SaslConfiguration> {

   private final AttributeSet attributes;
   private SaslAuthenticator saslAuthenticator;
   private Map<String, String> mechProperties = new HashMap<>();

   public SaslConfigurationBuilder() {
      this.attributes = SaslConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public SaslConfigurationBuilder authenticator(SaslAuthenticator saslAuthenticator) {
      this.saslAuthenticator = saslAuthenticator;
      return this;
   }

   public SaslConfigurationBuilder serverName(String name) {
      attributes.attribute(SaslConfiguration.SERVER_NAME).set(name);
      return this;
   }

   public String serverName() {
      return attributes.attribute(SaslConfiguration.SERVER_NAME).get();
   }

   public SaslConfigurationBuilder serverSubject(Subject subject) {
      attributes.attribute(SaslConfiguration.SERVER_SUBJECT).set(subject);
      return this;
   }

   public SaslConfigurationBuilder addQOP(String value) {
      Attribute<List<QOP>> attribute = attributes.attribute(SaslConfiguration.QOP);
      List<QOP> qops = attribute.get();
      qops.add(QOP.fromString(value));
      attribute.set(qops);
      return this;
   }

   public SaslConfigurationBuilder addStrength(String value) {
      Attribute<List<Strength>> attribute = attributes.attribute(SaslConfiguration.STRENGTH);
      List<Strength> strengths = attribute.get();
      strengths.add(Strength.fromString(value));
      attribute.set(strengths);
      return this;
   }

   public SaslConfigurationBuilder addPolicy(String value) {
      Attribute<List<Policy>> attribute = attributes.attribute(SaslConfiguration.POLICY);
      List<Policy> policies = attribute.get();
      policies.add(Policy.fromString(value));
      attribute.set(policies);
      return this;
   }

   public SaslConfigurationBuilder addProperty(String key, String value) {
      Attribute<Map<String, String>> a = attributes.attribute(SaslConfiguration.SASL_PROPERTIES);
      Map<String, String> map = a.get();
      map.put(key, value);
      a.set(map);
      return this;
   }

   private Map<String, String> getMechProperties() {
      if (mechProperties.isEmpty()) {
         mechProperties = new HashMap<>();
         List<QOP> qops = attributes.attribute(SaslConfiguration.QOP).get();
         if (!qops.isEmpty()) {
            String qopsValue = qops.stream().map(QOP::toString).collect(Collectors.joining(","));
            mechProperties.put(Sasl.QOP, qopsValue);
         }
         List<Strength> strengths = attributes.attribute(SaslConfiguration.STRENGTH).get();
         if (!strengths.isEmpty()) {
            String strengthsValue = strengths.stream().map(Strength::toString).collect(Collectors.joining(","));
            mechProperties.put(Sasl.STRENGTH, strengthsValue);
         }
         mechProperties.putAll(attributes.attribute(SaslConfiguration.SASL_PROPERTIES).get());
      }
      return mechProperties;
   }

   public SaslConfigurationBuilder addMechanisms(String... mechs) {
      for(String mech : mechs) {
         addAllowedMech(mech);
      }
      return this;
   }

   public SaslConfigurationBuilder addAllowedMech(String mech) {
      Attribute<Set<String>> attribute = attributes.attribute(SaslConfiguration.MECHANISMS);
      Set<String> mechs = attribute.get();
      mechs.add(mech);
      attribute.set(mechs);
      return this;
   }

   public boolean hasMechanisms() {
      return !attributes.attribute(SaslConfiguration.MECHANISMS).get().isEmpty();
   }

   public Set<String> mechanisms() {
      return attributes.attribute(SaslConfiguration.MECHANISMS).get();
   }

   @Override
   public void validate() {
      if (saslAuthenticator == null) {
         throw Log.CONFIG.saslAuthenticationProvider();
      }
      Set<String> allMechs = new LinkedHashSet<>(Arrays.asList(ExternalSaslServerFactory.NAMES));
      for (SaslServerFactory factory : SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), null, true)) {
         allMechs.addAll(Arrays.asList(factory.getMechanismNames(mechProperties)));
      }
      Attribute<Set<String>> mechanismAttr = attributes.attribute(SaslConfiguration.MECHANISMS);
      Set<String> allowedMechs = mechanismAttr.get();
      if (allowedMechs.isEmpty()) {
         mechanismAttr.set(allMechs);
      } else if (!allMechs.containsAll(allowedMechs)) {
         throw Log.CONFIG.invalidAllowedMechs(allowedMechs, allMechs);
      }
      if (attributes.attribute(SaslConfiguration.SERVER_NAME) == null) {
         throw Log.CONFIG.missingServerName();
      }
   }

   @Override
   public SaslConfiguration create() {
      Map<String, String> mechProperties = getMechProperties();
      return new SaslConfiguration(attributes.protect(), saslAuthenticator, mechProperties);
   }

   @Override
   public SaslConfigurationBuilder read(SaslConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      mechProperties = template.mechProperties();
      saslAuthenticator = template.authenticator();
      return this;
   }


   public SaslConfigurationBuilder addMechProperty(String key, String value) {
      mechProperties.put(key, value);
      return this;
   }

   public SaslConfigurationBuilder mechProperties(Map<String, String> mechProperties) {
      this.mechProperties = mechProperties;
      return this;
   }
}
