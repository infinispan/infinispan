package org.infinispan.server.configuration.security;

import java.security.Principal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.function.UnaryOperator;

import javax.security.auth.x500.X500Principal;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.realm.AggregateSecurityRealm;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class AggregateRealmConfiguration extends ConfigurationElement<AggregateRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "aggregate", String.class).immutable().build();
   static final AttributeDefinition<String> AUTHN_REALM = AttributeDefinition.builder(Attribute.AUTHENTICATION_REALM, null, String.class).immutable().build();
   static final AttributeDefinition<List<String>> AUTHZ_REALMS = AttributeDefinition.builder(Attribute.AUTHORIZATION_REALMS, null, (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().build();
   static final AttributeDefinition<NameRewriter> NAME_REWRITER = AttributeDefinition.builder(Element.NAME_REWRITER, NameRewriter.IDENTITY_REWRITER, NameRewriter.class).autoPersist(false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AggregateRealmConfiguration.class, NAME, AUTHN_REALM, AUTHZ_REALMS, NAME_REWRITER);
   }

   private EnumSet<ServerSecurityRealm.Feature> authenticationFeatures;

   AggregateRealmConfiguration(AttributeSet attributes) {
      super(Element.AGGREGATE_REALM, attributes);
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      domainBuilder.setDefaultRealmName(name()); // We make this the default realm

      String authenticationRealm = authenticationRealm();
      SecurityRealm authnRealm = realm.realms.get(authenticationRealm);
      if (authnRealm == null) {
         throw Server.log.unknownRealm(authenticationRealm);
      }
      authenticationFeatures = EnumSet.noneOf(ServerSecurityRealm.Feature.class);
      for (RealmProvider provider : realm.realmProviders()) {
         if (provider.name().equals(authenticationRealm)) {
            provider.applyFeatures(authenticationFeatures);
         }
      }

      List<String> names = authorizationRealms();
      SecurityRealm[] authzRealms;
      if (names.isEmpty()) { // we add all realms
         authzRealms = realm.realms.values().toArray(SecurityRealm[]::new);
      } else { // only the specified realms
         authzRealms = new SecurityRealm[names.size()];
         for (int i = 0; i < names.size(); i++) {
            SecurityRealm securityRealm = realm.realms.get(names.get(i));
            if (securityRealm == null) {
               throw Server.log.unknownRealm(names.get(i));
            } else {
               authzRealms[i] = securityRealm;
            }
         }
      }
      return new AggregateSecurityRealm(authnRealm, asPrincipalRewriter(nameRewriter()), authzRealms);
   }

   @Override
   public void applyFeatures(EnumSet<ServerSecurityRealm.Feature> features) {
      // We need to reset the features: only the authentication realm features matter
      features.retainAll(EnumSet.of(ServerSecurityRealm.Feature.ENCRYPT));
      features.addAll(authenticationFeatures);
   }

   static UnaryOperator<Principal> asPrincipalRewriter(NameRewriter rewriter) {
      return (principal) -> {
         if (principal == null) {
            return null;
         } else if (principal instanceof NamePrincipal || principal instanceof X500Principal) {
            String rewritten = rewriter.rewriteName(principal.getName());
            return rewritten == null ? null : new NamePrincipal(rewritten);
         } else {
            return principal;
         }
      };
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String authenticationRealm() {
      return attributes.attribute(AUTHN_REALM).get();
   }

   public List<String> authorizationRealms() {
      return attributes.attribute(AUTHZ_REALMS).get();
   }

   public NameRewriter nameRewriter() {
      return attributes.attribute(NAME_REWRITER).get();
   }
}
