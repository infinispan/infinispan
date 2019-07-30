package org.infinispan.server.logging;

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
   String userToolUsername();

   @Message(value = "Password: ")
   String userToolPassword();

   @Message(value = "User `%s` already exists. Overwrite (y/n) ? ")
   String userToolUserExists(String username);

   @Message(value = "Cannot add user `%s` without a password.")
   String userToolNoPassword(String username);

   @Message(value = "Binds the server to the specified address.")
   String serverHelpBindAddress();

   @Message(value = "Binds the server to the specified port. Defaults to `%d`.")
   String serverHelpBindPort(int defaultBindPort);

   @Message(value = "Displays usage information and exits.")
   String toolHelpHelp();

   @Message("Uses the specified configuration file. Defaults to `%s`.")
   String serverHelpServerConfig(String defaultConfiguration);

   @Message("Sets the name of the cluster. Defaults to `%s`.")
   String serverHelpClusterName(String defaultClusterName);

   @Message("Sets the name of the JGroups stack used for clustering. Defaults to `%s`.")
   String serverHelpClusterStack(String defaultStack);

   @Message("Sets the name of this node. Must be unique across the cluster.")
   String serverHelpNodeName();

   @Message("Adds the specified offset to all ports.")
   String serverHelpPortOffset();

   @Message("Uses the specified path as root for the server. Defaults to `%s`.")
   String toolHelpServerRoot(String defaultServerRoot);

   @Message("Displays version information and exits.")
   String toolHelpVersion();

   @Message("Sets a system property to the specified value.")
   String serverHelpProperty();

   @Message("Do not ask for confirmation when overwriting existing users.")
   String userToolHelpBatchMode();

   @Message("The name of the realm. Defaults to `%s`.")
   String userToolHelpRealm(String defaultRealmName);

   @Message("The name of the groups properties file relative to the server configuration path. Defaults to `%s`.")
   String userToolHelpGroupsFile(String defaultGroupsPropertiesFile);

   @Message("The name of the users properties file relative to the server configuration path. Defaults to `%s`.")
   String userToolHelpUsersFile(String defaultUsersPropertiesFile);

   @Message("Comma-separated list of groups to add to the user.")
   String userToolHelpGroups();

   @Message("The password of the user.")
   String userToolHelpPassword();

   @Message("The name of the user to add.")
   String userToolHelpUser();
}
