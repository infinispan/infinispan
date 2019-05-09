package org.infinispan.server.logging;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "%s Server starting", id = 80000)
   void serverStarting(String name);

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
   @Message(value = "Protocol %s listening on %s:%d", id = 80004)
   void protocolStarted(String name, String host, int port);

   @Message(value = "Duplicate path '%s'", id = 80005)
   CacheConfigurationException duplicatePath(String name);

   @Message(value = "Duplicate network interface '%s'", id = 80006)
   CacheConfigurationException duplicateNetworkInterface(String name);

   @Message(value = "Duplicate socket binding '%s'", id = 80007)
   CacheConfigurationException duplicateSocketBinding(String name);

   @Message(value = "Cannot instantiate protocol server configuration '%s'", id = 80008)
   CacheConfigurationException cannotInstantiateProtocolServerConfigurationBuilder(Class<?> klass, @Cause Exception e);

   @Message(value = "Unknown interface '%s'", id = 80009)
   CacheConfigurationException unknownInterface(String interfaceName);

   @Message(value = "Unknown socket binding '%s'", id = 80010)
   CacheConfigurationException unknownSocketBinding(String value);

   @Message(value = "The path '%s' is not absolute", id = 80011)
   CacheConfigurationException nonAbsolutePath(String path);

   @Message(value = "Duplicate security domain '%s'", id = 80012)
   CacheConfigurationException duplicateSecurityDomain(String name);

   @Message(value = "Duplicate realm type '%s' in realm '%s'", id = 80013)
   CacheConfigurationException duplicateRealmType(String type, String name);

   @Message(value = "Unknown security domain '%s'", id = 80014)
   CacheConfigurationException unknownSecurityDomain(String name);

   @Message(value = "Unable to load realm property files", id = 80015)
   CacheConfigurationException unableToLoadRealmPropertyFiles(@Cause IOException e);

   @Message(value = "No default key manager available", id = 80016)
   NoSuchAlgorithmException noDefaultKeyManager();

   @LogMessage(level = Logger.Level.INFO)
   @Message(value = "Server configuration: %s", id = 80017)
   void serverConfiguration(String name);
}
