package org.infinispan.server.logging;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * @since 10.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(MethodHandles.lookup(), Messages.class);

   @Message(value = "Unrecognized command-line argument `%s`.", id = 90001)
   String unknownArgument(String argument);

   @Message(value = "Invalid argument `%s`. Arguments must be prefixed with either - or --.", id = 90002)
   String invalidArgument(String argument);

   @Message(value = "Invalid argument `%s`. The - prefix must be used only for single-character arguments.", id = 90003)
   String invalidShortArgument(String command);

   @Message(value = "Binds the server endpoint to a specific address.")
   String serverHelpBindAddress();

   @Message(value = "Binds the server to a specific port. Defaults to `%d`.")
   String serverHelpBindPort(int defaultBindPort);

   @Message(value = "Displays usage information and exits.")
   String toolHelpHelp();

   @Message("Specifies a server configuration file. Defaults to `%s`. Can be repeated, in which case the configurations are layered.")
   String serverHelpServerConfig(String defaultConfiguration);

   @Message("Specifies a logging configuration file. Defaults to `%s`.")
   String serverHelpLoggingConfig(String defaultConfiguration);

   @Message("Sets the name of the cluster. Default set by configuration expression")
   String serverHelpClusterName();

   @Message("Specifies the JGroups stack for clustering. Default set by configuration expression")
   String serverHelpClusterStack();

   @Message("Specifies the JGroups bind address for clustering.")
   String serverHelpClusterAddress();

   @Message("Sets the name of this node. Must be unique across the cluster.")
   String serverHelpNodeName();

   @Message("Adds a numeric offset to all ports.")
   String serverHelpPortOffset();

   @Message("Specifies the root path for the server. Defaults to `%s`.")
   String toolHelpServerRoot(String defaultServerRoot);

   @Message("Displays version information and exits.")
   String toolHelpVersion();

   @Message("Sets a system property to the specified value.")
   String serverHelpProperty();

   @Message("Sets system properties from the specified file.")
   String serverHelpProperties();

   @Message("Activate the JVM AOT mode to reduce startup time and initial memory footprint. Requires JDK 24 or greater.")
   String serverHelpAOT();

   @Message("Activate debug mode with an optional argument to override the default port (8787).")
   String serverHelpDebug();

   @Message("Activate JMX remoting with an optional argument to override the default port (9999).")
   String serverHelpJMX();

   @Message("CLI batch script to execute before server startup. Can be specified multiple times.")
   String serverHelpPreStartBatch();
}
