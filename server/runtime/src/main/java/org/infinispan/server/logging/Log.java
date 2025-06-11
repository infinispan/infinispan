package org.infinispan.server.logging;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import javax.naming.NamingException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.OS;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.IpSubnetFilterRule;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;
import org.wildfly.security.auth.server.RealmUnavailableException;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 80000, max = 81000)
public interface Log extends BasicLogger {
   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "%s Server %s starting", id = 80000)
   void serverStarting(String name, String version);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "%s Server %s started in %dms", id = 80001)
   void serverStarted(String name, String version, long ms);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "%s Server stopping", id = 80002)
   void serverStopping(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "%s Server stopped", id = 80003)
   void serverStopped(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector %s (%s) listening on %s:%d", id = 80004)
   void protocolStarted(String name, String socketBinding, String host, int port);

//   @Message(value = "Duplicate path '%s'", id = 80005)
//   CacheConfigurationException duplicatePath(String name);
//
//   @Message(value = "Duplicate network interface '%s'", id = 80006)
//   CacheConfigurationException duplicateNetworkInterface(String name);
//
//   @Message(value = "Duplicate socket binding '%s'", id = 80007)
//   CacheConfigurationException duplicateSocketBinding(String name);

   @Message(value = "Cannot instantiate protocol server configuration '%s'", id = 80008)
   CacheConfigurationException cannotInstantiateProtocolServerConfigurationBuilder(Class<?> klass, @Cause Exception e);

   @Message(value = "Unknown interface '%s'", id = 80009)
   CacheConfigurationException unknownInterface(String interfaceName);

   @Message(value = "Unknown socket binding '%s'", id = 80010)
   CacheConfigurationException unknownSocketBinding(String value);

//   @Message(value = "The path '%s' is not absolute", id = 80011)
//   CacheConfigurationException nonAbsolutePath(String path);
//
//   @Message(value = "Duplicate security domain '%s'", id = 80012)
//   CacheConfigurationException duplicateSecurityRealm(String name);
//
//   @Message(value = "Duplicate realm type '%s' in realm '%s'", id = 80013)
//   CacheConfigurationException duplicateRealmType(String type, String name);

   @Message(value = "Unknown security domain '%s'", id = 80014)
   CacheConfigurationException unknownSecurityDomain(String name);

   @Message(value = "Unable to load realm property files", id = 80015)
   CacheConfigurationException unableToLoadRealmPropertyFiles(@Cause IOException e);

   @Message(value = "No default key manager available", id = 80016)
   NoSuchAlgorithmException noDefaultKeyManager();

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Server configuration: %s", id = 80017)
   void serverConfiguration(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector %s", id = 80018)
   void protocolStarted(ProtocolServer<?> connector);

//   @LogMessage(level = Logger.Level.INFO)
//   @Message(value = "Protocol Router listening on %s:%d", id = 80019)
//   void routerStarted(String host, int port);

//   @Message(value = "Cannot use a trust store without a server identity", id = 80020)
//   CacheConfigurationException trustStoreWithoutServerIdentity();

   @Message(value = "Authentication cannot be configured without a security realm", id = 80021)
   CacheConfigurationException authenticationWithoutSecurityRealm();

   @Message(value = "Cannot configure protocol encryption under a single port endpoint. Use a dedicated socket binding.", id = 80022)
   CacheConfigurationException cannotConfigureProtocolEncryptionUnderSinglePort();

   @Message(value = "Cannot configure a protocol with the same socket binding used by the endpoint. Use a dedicated socket binding.", id = 80023)
   CacheConfigurationException protocolCannotUseSameSocketBindingAsEndpoint();

   @Message(value = "Invalid URL: '%s'", id = 80024)
   CacheConfigurationException invalidUrl(String url);

   @Message(value = "Cannot have multiple connectors of the same type: found [%s]", id = 80025)
   CacheConfigurationException multipleEndpointsSameTypeFound(String names);

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "Extension factory '%s' is lacking a @NamedFactory annotation", id = 80026)
   void unnamedFactoryClass(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Loaded extension '%s'", id = 80027)
   void loadedExtension(String name);

   @LogMessage(level = Logger.Level.FATAL)
   @Message(value = "%s Server failed to start", id = 80028)
   void serverFailedToStart(String name, @Cause Throwable t);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Cluster shutdown", id = 80029)
   void clusterShutdown();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "Clustered task error", id = 80030)
   void clusteredTaskError(@Cause Throwable t);

   @Message(value = "Unknown server identity '%s'", id = 80031)
   IllegalArgumentException unknownServerIdentity(String serverPrincipal);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Logging configuration: %s", id = 80032)
   void loggingConfiguration(String path);

   @Message(value = "Cannot find a network address which matches the supplied configuration", id = 80033)
   CacheConfigurationException invalidNetworkConfiguration();

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Server '%s' listening on %s://%s:%d", id = 80034)
   void endpointUrl(Object name, String proto, String host, int port);

   @Message(value = "Unknown appender `%s`", id = 80035)
   IllegalArgumentException unknownAppender(String appender);

   @Message(value = "Invalid level `%s`", id = 80036)
   IllegalArgumentException invalidLevel(String level);

   @Message(value = "The name '%s' is already bound", id = 80037)
   NamingException nameAlreadyBound(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Created datasource '%s' bound to JNDI '%s'", id = 80038)
   void dataSourceCreated(String name, String jndiName);

   //@Message(value = "Invalid Unicode sequence '%s'", id = 80039)
   //IOException invalidUnicodeSequence(String sequence, @Cause NoSuchElementException e);

   @Message(value = "No realm name found in users property file - non-plain-text users file must contain \"#$REALM_NAME=RealmName$\" line", id = 80040)
   RealmUnavailableException noRealmFoundInProperties();

   @Message(value = "Duplicate data source '%s'", id = 80041)
   CacheConfigurationException duplicateDataSource(String name);

   @Message(value = "Duplicate JNDI name '%s'", id = 80042)
   CacheConfigurationException duplicateJndiName(String jndiName);

   @Message(value = "Cannot generate the server report on %s", id = 80043)
   IllegalStateException serverReportUnavailable(OS os);

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "HotRod lazy-retrieval has been deprecated and will be removed in a future version with no direct replacement", id = 80044)
   void warnHotRodLazyRetrievalDeprecated();

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "Overriding existing endpoint on socket-binding '%s'", id = 80045)
   void endpointSocketBindingOverride(String name);

   @Message(value = "Unknown credential store '%s'", id = 80046)
   IllegalArgumentException unknownCredentialStore(String store);

   @Message(value = "Unknown credential '%s' in store '%s'", id = 80047)
   IllegalArgumentException unknownCredential(String alias, String store);

   @Message(value = "A credential store name is missing", id = 80048)
   IllegalArgumentException missingCredentialStoreName();

   @Message(value = "A credential alias must be specified if the store doesn't contain a single alias", id = 80049)
   CacheConfigurationException unspecifiedCredentialAlias();

   @Message(value = "Credential reference in element '%s' would override value specified in attribute '%s'", id = 80050)
   CacheConfigurationException cannotOverrideCredential(String element, String attribute);

   @Message(value = "Element '%s' is missing a credential attribute '%s' or a credential reference", id = 80051)
   CacheConfigurationException missingCredential(String element, String attribute);

   @Message(value = "The request authentication mechanism '%s' is not supported", id = 80052)
   SecurityException unsupportedMechanism(String mechName);

   @Message(value = "Encryption cannot be configured without a security realm", id = 80053)
   CacheConfigurationException encryptionWithoutSecurityRealm();

   @Message(value = "No default trust manager available", id = 80054)
   CacheConfigurationException noDefaultTrustManager();

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector '%s': IP filter set: %s", id = 80055)
   void connectorIpFilterSet(String connector, List<IpSubnetFilterRule> rules);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector '%s': IP filter cleared", id = 80056)
   void connectorIpFilterCleared(String connector);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector '%s': stopped", id = 80057)
   void connectorStopped(String connector);

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Connector '%s': started", id = 80058)
   void connectorStarted(String connector);

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "No script engines are available", id = 80059)
   void noScriptEngines();

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "SSL Transport using realm '%s`", id = 80060)
   void sslTransport(String value);

   @Message(value = "Realm '%s' does not provide an SSL context", id = 80061)
   IllegalStateException noSSLContext(String realm);

   @Message(value = "Cannot add multiple realms of type '%s' to security realm '%s'", id = 80062)
   CacheConfigurationException duplicateRealmType(String type, String realm);

   @Message(value = "Duplicate realm '%s'", id = 80063)
   CacheConfigurationException duplicateRealm(String realmName);

   @Message(value = "Unknown realm '%s'", id = 80064)
   CacheConfigurationException unknownRealm(String realmName);

   @Message(value = "Invalid keystore '%s'", id = 80065)
   KeyStoreException invalidKeyStore(String filename);

   @Message(value = "KeyStore definitions without a path must specify a type.", id = 80066)
   CacheConfigurationException filelessKeyStoreRequiresType();

   @Message(value = "The RESP endpoint requires a security realm which supports passwords or a trustore realm.", id = 80067)
   CacheConfigurationException respEndpointRequiresRealmWithPasswordOrTrustore();

   @Message(value = "Wrong masked password format", id = 80068)
   CacheConfigurationException wrongMaskedPasswordFormat();

   @Message(value = "Alias '%s' not in keystore '%s'", id = 80069)
   CacheConfigurationException aliasNotInKeystore(String alias, String keystore);

   @Message(value = "The cache container requires authorization, but none of the connectors enable authentication", id = 80070)
   CacheConfigurationException authorizationWithoutAuthentication();

   @Message(value = "A user mapper must be configured in order to use LDAP direct verification", id = 80071)
   CacheConfigurationException ldapDirectVerificationWithoutUserMapper();

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "JMX remoting enabled without a default security realm. All connections will be rejected.", id = 80072)
   void jmxNoDefaultSecurityRealm();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(value = "JMX authentication error", id = 80073)
   void jmxAuthenticationError(@Cause Throwable e);

   @Message(value = "The Memcached endpoint text protocol requires a security realm which supports passwords.", id = 80074)
   CacheConfigurationException memcachedTextEndpointRequiresRealmWithPassword();

   @LogMessage(level = Logger.Level.WARN)
   @Message(value = "The '%2$s' certificate in keystore '%1$s' does not have the subjectAltName extension as recommended by RFC2818.", id = 80075)
   void serverCertificateWithoutSAN(String keyStoreFileName, String alias);

   @Message(value = "Malformed entry in user properties file at line %d", id = 80076)
   IllegalStateException malformedUserProperties(int line);
}
