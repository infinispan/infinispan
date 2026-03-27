package org.infinispan.server.configuration.security;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.MapAttributes;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

/**
 * @since 10.0
 */
public class LocalRealmConfiguration extends ConfigurationElement<LocalRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "local", String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LocalRealmConfiguration.class, NAME);
   }

   LocalRealmConfiguration(AttributeSet attributes) {
      super(Element.LOCAL_REALM, attributes);
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      String localUser = name();
      return new SecurityRealm() {
         @Override
         public RealmIdentity getRealmIdentity(Principal principal) throws RealmUnavailableException {
            return new RealmIdentity() {
               @Override
               public Principal getRealmIdentityPrincipal() {
                  return principal;
               }

               @Override
               public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
                  return SupportLevel.UNSUPPORTED;
               }

               @Override
               public <C extends Credential> C getCredential(Class<C> credentialType) {
                  return null;
               }

               @Override
               public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
                  return SupportLevel.UNSUPPORTED;
               }

               @Override
               public boolean verifyEvidence(Evidence evidence) {
                  return false;
               }

               @Override
               public boolean exists() {
                  return true;
               }

               @Override
               public AuthorizationIdentity getAuthorizationIdentity() {
                  MapAttributes attrs = new MapAttributes();
                  attrs.addAll("Roles", Collections.singletonList("admin"));
                  return AuthorizationIdentity.basicIdentity(attrs);
               }
            };
         }

         @Override
         public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
            return SupportLevel.UNSUPPORTED;
         }

         @Override
         public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
            return SupportLevel.UNSUPPORTED;
         }
      };
   }

   @Override
   public void applyFeatures(EnumSet<ServerSecurityRealm.Feature> features) {
      features.add(ServerSecurityRealm.Feature.LOCAL);
   }
}
