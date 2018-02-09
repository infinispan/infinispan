package org.infinispan.client.hotrod.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TypedProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.commons.util.Util;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthenticationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthenticationConfiguration> {
   private static final Log log = LogFactory.getLog(AuthenticationConfigurationBuilder.class);
   public static final String DEFAULT_REALM = "ApplicationRealm";
   private CallbackHandler callbackHandler;
   private boolean enabled = false;
   private String serverName;
   private Map<String, String> saslProperties = new HashMap<>();
   private String saslMechanism;
   private Subject clientSubject;
   private String username;
   private char[] password;
   private String realm;

   public AuthenticationConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Specifies a {@link CallbackHandler} to be used during the authentication handshake.
    * The {@link Callback}s that need to be handled are specific to the chosen SASL mechanism.
    */
   public AuthenticationConfigurationBuilder callbackHandler(CallbackHandler callbackHandler) {
      this.callbackHandler = callbackHandler;
      return this;
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
    * Selects the SASL mechanism to use for the connection to the server.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslMechanism(String saslMechanism) {
      this.saslMechanism = saslMechanism;
      return enable();
   }

   /**
    * Sets the SASL properties.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslProperties(Map<String, String> saslProperties) {
      this.saslProperties = saslProperties;
      return enable();
   }

   /**
    * Sets the SASL QOP property. If multiple values are specified they will determine preference order.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslQop(SaslQop... qop) {
      StringBuilder s = new StringBuilder();
      for(int i=0; i < qop.length; i++) {
         if (i > 0) {
            s.append(",");
         }
         s.append(qop[i].toString());
      }
      this.saslProperties.put(Sasl.QOP, s.toString());
      return enable();
   }

   /**
    * Sets the SASL strength property. If multiple values are specified they will determine preference order.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder saslStrength(SaslStrength... strength) {
      StringBuilder s = new StringBuilder();
      for(int i=0; i < strength.length; i++) {
         if (i > 0) {
            s.append(",");
         }
         s.append(strength[i].toString());
      }
      this.saslProperties.put(Sasl.STRENGTH, s.toString());
      return enable();
   }

   /**
    * Sets the name of the server as expected by the SASL protocol
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder serverName(String serverName) {
      this.serverName = serverName;
      return enable();
   }

   /**
    * Sets the client subject, necessary for those SASL mechanisms which require it to access client credentials (i.e. GSSAPI).
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder clientSubject(Subject clientSubject) {
      this.clientSubject = clientSubject;
      return enable();
   }

   /**
    * Specifies the username to be used for authentication. This will use a simple CallbackHandler.
    * This is mutually exclusive with explicitly providing the CallbackHandler.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder username(String username) {
      this.username = username;
      return enable();
   }

   /**
    * Specifies the password to be used for authentication. A username is also required.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder password(String password) {
      this.password = password != null ? password.toCharArray() : null;
      return enable();
   }

   /**
    * Specifies the password to be used for authentication. A username is also required.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder password(char[] password) {
      this.password = password;
      return enable();
   }

   /**
    * Specifies the realm to be used for authentication. Username and password also need to be supplied. If none
    * is specified, this defaults to 'ApplicationRealm'.
    * Setting this property also implicitly enables authentication (see {@link #enable()}
    */
   public AuthenticationConfigurationBuilder realm(String realm) {
      this.realm = realm;
      return enable();
   }

   @Override
   public AuthenticationConfiguration create() {
      String mech = saslMechanism == null ? "DIGEST-MD5" : saslMechanism;
      CallbackHandler cbh;
      if (username != null) {
         cbh = new BasicCallbackHandler(username, realm != null ? realm : DEFAULT_REALM, password);
      } else if ("EXTERNAL".equals(mech) && callbackHandler == null) {
         cbh = new VoidCallbackHandler();
      } else {
         cbh = callbackHandler;
      }
      return new AuthenticationConfiguration(cbh, clientSubject, enabled, mech, saslProperties, serverName);
   }

   @Override
   public Builder<?> read(AuthenticationConfiguration template) {
      this.callbackHandler = template.callbackHandler();
      this.clientSubject = template.clientSubject();
      this.enabled = template.enabled();
      this.saslMechanism = template.saslMechanism();
      this.saslProperties = template.saslProperties();
      this.serverName = template.serverName();
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (callbackHandler == null && clientSubject == null && username == null && !"EXTERNAL".equals(saslMechanism)) {
            throw log.invalidCallbackHandler();
         }
         if (callbackHandler != null && username != null) {
            throw log.callbackHandlerAndUsernameMutuallyExclusive();
         }
         if (saslMechanism == null) {
            throw log.invalidSaslMechanism(saslMechanism);
         }
      }
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(ConfigurationProperties.SASL_MECHANISM))
        saslMechanism(typed.getProperty(ConfigurationProperties.SASL_MECHANISM, saslMechanism, true));

      Object prop = typed.get(ConfigurationProperties.AUTH_CALLBACK_HANDLER);
      if (prop instanceof String) {
         String cbhClassName = StringPropertyReplacer.replaceProperties((String) prop);
         CallbackHandler handler = Util.getInstance(cbhClassName, builder.getBuilder().classLoader());
         this.callbackHandler(handler);
      } else if (prop instanceof CallbackHandler) {
         this.callbackHandler((CallbackHandler) prop);
      }

      if (typed.containsKey(ConfigurationProperties.AUTH_USERNAME))
         username(typed.getProperty(ConfigurationProperties.AUTH_USERNAME, username, true));

      if (typed.containsKey(ConfigurationProperties.AUTH_PASSWORD))
         password(typed.getProperty(ConfigurationProperties.AUTH_PASSWORD, null, true));

      if (typed.containsKey(ConfigurationProperties.AUTH_REALM))
         realm(typed.getProperty(ConfigurationProperties.AUTH_REALM, realm, true));

      if (typed.containsKey(ConfigurationProperties.AUTH_SERVER_NAME))
         serverName(typed.getProperty(ConfigurationProperties.AUTH_SERVER_NAME, serverName, true));

      if (typed.containsKey(ConfigurationProperties.AUTH_CLIENT_SUBJECT))
         this.clientSubject((Subject) typed.get(ConfigurationProperties.AUTH_CLIENT_SUBJECT));

      Map<String, String> saslProperties = typed.entrySet().stream()
            .filter(e -> ((String) e.getKey()).startsWith(ConfigurationProperties.SASL_PROPERTIES_PREFIX))
            .collect(Collectors.toMap(
                  e -> ConfigurationProperties.SASL_PROPERTIES_PREFIX_REGEX
                        .matcher((String) e.getKey()).replaceFirst(""),
                  e -> StringPropertyReplacer.replaceProperties((String) e.getValue())));
      if (!saslProperties.isEmpty())
         this.saslProperties(saslProperties);

      if (typed.containsKey(ConfigurationProperties.USE_AUTH))
         this.enabled(typed.getBooleanProperty(ConfigurationProperties.USE_AUTH, enabled, true));

      return builder.getBuilder();
   }

}
