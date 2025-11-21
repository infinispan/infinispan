package org.infinispan.client.openapi.configuration;

import java.util.Properties;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public class AuthenticationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthenticationConfiguration> {
   private boolean enabled = false;
   private String mechanism;
   private Subject clientSubject;
   private String username;
   private char[] password;
   private String realm;

   public AuthenticationConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Configures whether authentication should be enabled or not
    */
   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Enables authentication
    */
   public AuthenticationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Disables authentication
    */
   public AuthenticationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * Selects the authentication mechanism to use for the connection to the server. Setting this property also
    * implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder mechanism(String mechanism) {
      this.mechanism = mechanism;
      return enable();
   }

   /**
    * Sets the client subject, necessary for those mechanisms which require it to access client credentials (i.e.
    * SPNEGO). Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder clientSubject(Subject clientSubject) {
      this.clientSubject = clientSubject;
      return enable();
   }

   /**
    * Specifies the username to be used for authentication. This will use a simple CallbackHandler. This is mutually
    * exclusive with explicitly providing the CallbackHandler. Setting this property also implicitly enables
    * authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder username(String username) {
      this.username = username;
      return enable();
   }

   /**
    * Specifies the password to be used for authentication. A username is also required. Setting this property also
    * implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder password(String password) {
      this.password = password != null ? password.toCharArray() : null;
      return enable();
   }

   /**
    * Specifies the password to be used for authentication. A username is also required. Setting this property also
    * implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder password(char[] password) {
      this.password = password;
      return enable();
   }

   /**
    * Specifies the realm to be used for authentication. Username and password also need to be supplied. If none is
    * specified, this defaults to 'ApplicationRealm'. Setting this property also implicitly enables authentication (see
    * {@link #enable()}
    */
   public AuthenticationConfigurationBuilder realm(String realm) {
      this.realm = realm;
      return enable();
   }

   @Override
   public AuthenticationConfiguration create() {
      String mech = mechanism == null ? "AUTO" : mechanism;
      return new AuthenticationConfiguration(clientSubject, enabled, mech, realm, username, password);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template, Combine combine) {
      this.username = template.username();
      this.password = template.password();
      this.clientSubject = template.clientSubject();
      this.enabled = template.enabled();
      this.mechanism = template.mechanism();
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(OpenAPIClientConfigurationProperties.AUTH_MECHANISM))
         mechanism(typed.getProperty(OpenAPIClientConfigurationProperties.AUTH_MECHANISM, mechanism, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.AUTH_USERNAME))
         username(typed.getProperty(OpenAPIClientConfigurationProperties.AUTH_USERNAME, username, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.AUTH_PASSWORD))
         password(typed.getProperty(OpenAPIClientConfigurationProperties.AUTH_PASSWORD, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.AUTH_REALM))
         realm(typed.getProperty(OpenAPIClientConfigurationProperties.AUTH_REALM, realm, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.AUTH_CLIENT_SUBJECT))
         this.clientSubject((Subject) typed.get(OpenAPIClientConfigurationProperties.AUTH_CLIENT_SUBJECT));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.USE_AUTH))
         this.enabled(typed.getBooleanProperty(OpenAPIClientConfigurationProperties.USE_AUTH, enabled, true));

      return builder.getBuilder();
   }

}
