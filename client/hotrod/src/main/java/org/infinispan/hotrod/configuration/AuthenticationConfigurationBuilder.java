package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.CALLBACK_HANDLER;
import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.CLIENT_SUBJECT;
import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.ENABLED;
import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.SASL_MECHANISM;
import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.SASL_PROPERTIES;
import static org.infinispan.hotrod.configuration.AuthenticationConfiguration.SERVER_NAME;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.Map;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.hotrod.impl.security.BasicCallbackHandler;
import org.infinispan.hotrod.impl.security.TokenCallbackHandler;
import org.infinispan.hotrod.impl.security.VoidCallbackHandler;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @since 14.0
 */
public class AuthenticationConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<AuthenticationConfiguration> {
   private final AttributeSet attributes = AuthenticationConfiguration.attributeDefinitionSet();
   public static final String DEFAULT_REALM = "default";
   private static final String EXTERNAL_MECH = "EXTERNAL";
   private static final String OAUTHBEARER_MECH = "OAUTHBEARER";
   private static final String GSSAPI_MECH = "GSSAPI";
   private static final String GS2_KRB5_MECH = "GS2-KRB5";

   private String username;
   private char[] password;
   private String realm;
   private String token;

   public AuthenticationConfigurationBuilder(HotRodConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Specifies a {@link CallbackHandler} to be used during the authentication handshake. The {@link Callback}s that
    * need to be handled are specific to the chosen SASL mechanism.
    */
   public AuthenticationConfigurationBuilder callbackHandler(CallbackHandler callbackHandler) {
      attributes.attribute(CALLBACK_HANDLER).set(callbackHandler);
      return this;
   }

   /**
    * Configures whether authentication should be enabled or not
    */
   public AuthenticationConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enables authentication
    */
   public AuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   /**
    * Disables authentication
    */
   public AuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   /**
    * Selects the SASL mechanism to use for the connection to the server. Setting this property also implicitly enables
    * authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslMechanism(String saslMechanism) {
      attributes.attribute(SASL_MECHANISM).set(saslMechanism);
      return enable();
   }

   /**
    * Sets the SASL properties. Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslProperties(Map<String, String> saslProperties) {
      attributes.attribute(SASL_PROPERTIES).set(saslProperties);
      return enable();
   }

   /**
    * Sets the SASL QOP property. If multiple values are specified they will determine preference order. Setting this
    * property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslQop(SaslQop... qop) {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < qop.length; i++) {
         if (i > 0) {
            s.append(",");
         }
         s.append(qop[i].toString());
      }
      attributes.attribute(SASL_PROPERTIES).get().put(Sasl.QOP, s.toString());
      return enable();
   }

   /**
    * Sets the SASL strength property. If multiple values are specified they will determine preference order. Setting
    * this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslStrength(SaslStrength... strength) {
      StringBuilder s = new StringBuilder();
      for (int i = 0; i < strength.length; i++) {
         if (i > 0) {
            s.append(",");
         }
         s.append(strength[i].toString());
      }
      attributes.attribute(SASL_PROPERTIES).get().put(Sasl.STRENGTH, s.toString());
      return enable();
   }

   /**
    * Sets the name of the server as expected by the SASL protocol Setting this property also implicitly enables
    * authentication (see {@link #enable()} This defaults to "infinispan"
    */
   public AuthenticationConfigurationBuilder serverName(String serverName) {
      attributes.attribute(SERVER_NAME).set(serverName);
      return enable();
   }

   /**
    * Sets the client subject, necessary for those SASL mechanisms which require it to access client credentials (i.e.
    * GSSAPI). Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder clientSubject(Subject clientSubject) {
      attributes.attribute(CLIENT_SUBJECT).set(clientSubject);
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
    * specified, this defaults to {@link #DEFAULT_REALM}. Setting this property also implicitly enables authentication
    * (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder realm(String realm) {
      this.realm = realm;
      return enable();
   }

   public AuthenticationConfigurationBuilder token(String token) {
      this.token = token;
      return enable();
   }

   @Override
   public AuthenticationConfiguration create() {
      String mech = attributes.attribute(SASL_MECHANISM).get();
      CallbackHandler cbh = attributes.attribute(CALLBACK_HANDLER).get();
      if (cbh == null) {
         if (OAUTHBEARER_MECH.equals(mech)) {
            attributes.attribute(CALLBACK_HANDLER).set(new TokenCallbackHandler(token));
         } else if (username != null) {
            attributes.attribute(CALLBACK_HANDLER).set(new BasicCallbackHandler(username, realm != null ? realm : DEFAULT_REALM, password));
         } else if (EXTERNAL_MECH.equals(mech) || GSSAPI_MECH.equals(mech) || GS2_KRB5_MECH.equals(mech)) {
            attributes.attribute(CALLBACK_HANDLER).set(new VoidCallbackHandler());
         }
      }
      return new AuthenticationConfiguration(attributes.protect());
   }

   @Override
   public AuthenticationConfigurationBuilder read(AuthenticationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get()) {
         Attribute<CallbackHandler> cbh = attributes.attribute(CALLBACK_HANDLER);
         Attribute<String> mech = attributes.attribute(SASL_MECHANISM);
         if (cbh.isNull() && attributes.attribute(CLIENT_SUBJECT).isNull() && username == null && token == null && !EXTERNAL_MECH.equals(mech)) {
            throw HOTROD.invalidAuthenticationConfiguration();
         }
         if (OAUTHBEARER_MECH.equals(mech) && cbh.isNull() && token == null) {
            throw HOTROD.oauthBearerWithoutToken();
         }
         if (!cbh.isNull() && (username != null || token != null)) {
            throw HOTROD.callbackHandlerAndUsernameMutuallyExclusive();
         }
      }
   }

   @Override
   public HotRodConfigurationBuilder withProperties(Properties properties) {
      attributes.fromProperties(TypedProperties.toTypedProperties(properties), "org.infinispan.client.");
      return builder;
   }

}
