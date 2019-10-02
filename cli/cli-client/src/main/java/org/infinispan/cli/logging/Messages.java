package org.infinispan.cli.logging;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.AccessDeniedException;

import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * @since 10.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

   @Message(value = "Unrecognized command-line argument `%s`.", id = 90001)
   String unknownArgument(String argument);

   @Message(value = "Invalid argument `%s`. Arguments must be prefixed with either - or --.", id = 90002)
   String invalidArgument(String argument);

   @Message(value = "Invalid argument `%s`. The - prefix must be used only for single-character arguments.", id = 90003)
   String invalidShortArgument(String command);

   @Message(value = "Username: ")
   String username();

   @Message(value = "Password: ")
   String password();

   @Message(value = "Displays usage information and exits.")
   String cliHelpHelp();

   @Message("Displays version information and exits.")
   String cliHelpVersion();

   @Message("File '%s' doesn't exist or is not a file")
   String fileNotExists(String inputFile);

   @Message("Connects to a remote %s instance\n")
   String cliHelpConnect(String brandName);

   @Message("Server HTTP http://[username[:password]]@host:port")
   String cliHelpConnectHTTP();

   @Message("Reads input from the specified file instead of using interactive mode. If FILE is '-', then commands will be read from stdin.")
   String cliHelpFile();

   @Message("The password of an optional truststore to be used for SSL/TLS connections.")
   String cliHelpTruststorePassword();

   @Message("The path of an optional truststore to be used for SSL/TLS connections.")
   String cliHelpTruststore();

   @Message("Trusts all certificates in SSL/TLS connections.")
   String cliHelpTrustAll();

   @Message("Not Found %s")
   IOException notFound(String s);

   @Message("The supplied credentials are invalid %s")
   AccessDeniedException unauthorized(String s);

   @Message("Error: %s")
   IOException error(String s);

   @Message("The user is not allowed to access the server resource: %s")
   AccessDeniedException forbidden(String s);

   @Message("Error while loading trust store '%s'")
   String keyStoreError(String trustStorePath, @Cause Exception e);

   @Message("No such resource '%s'")
   IllegalArgumentException noSuchResource(String name);

   @Message("Command invoked from the wrong context")
   IllegalStateException illegalContext();

   @Message("Illegal arguments for command")
   IllegalArgumentException illegalCommandArguments();

   @Message("The options '%s' and '%s' are mutually exclusive")
   IllegalArgumentException mutuallyExclusiveOptions(String arg1, String arg2);

   @Message("One of the '%s' and '%s' options are required")
   IllegalArgumentException requiresOneOf(String arg1, String arg2);

   @Message("Could not connect to server: %s")
   ConnectException connectionFailed(String message);
}
