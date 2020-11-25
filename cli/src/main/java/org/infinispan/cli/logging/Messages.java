package org.infinispan.cli.logging;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import org.infinispan.cli.patching.PatchInfo;
import org.infinispan.cli.patching.PatchOperation;
import org.infinispan.cli.user.UserTool;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * @since 10.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);

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

   @Message("Error: %s")
   RuntimeException genericError(String s, @Cause Throwable t);

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

   @Message("Invalid resource '%s'")
   IllegalArgumentException invalidResource(String name);

   @Message("No patches installed")
   String patchNoPatchesInstalled();

   @Message("%s")
   String patchInfo(PatchInfo patchInfo);

   @Message("The supplied patch cannot be applied to %s %s")
   IllegalStateException patchCannotApply(String brandName, String version);

   @Message("File %s SHA mismatch. Expected = %s, Actual = %s")
   String patchShaMismatch(Path path, String digest, String sha256);

   @Message("The following errors were encountered while validating the installation:%n%s")
   IllegalStateException patchValidationErrors(String errors);

   @Message("No installed patches to roll back")
   IllegalStateException patchNoPatchesInstalledToRollback();

   @Message("Cannot find the infinispan-commons jar under %s")
   IllegalStateException patchCannotFindCommons(Path lib);

   @Message("Cannot create patch %s with patches for %s")
   IllegalStateException patchIncompatibleProduct(String localBrand, String patchBrand);

   @Message("Could not write patches file")
   IllegalStateException patchCannotWritePatchesFile(@Cause IOException e);

   @Message("Rolled back patch %s")
   String patchRollback(PatchInfo patchInfo);

   @Message("[Dry run] ")
   String patchDryRun();

   @Message("Backing up '%s' to '%s'")
   String patchBackup(Path from, Path to);

   @Message("Error while creating patch")
   RuntimeException patchCreateError(@Cause IOException e);

   @Message("Adding file '%s'")
   String patchCreateAdd(Path target);

   @Message("Rolling back file '%s'")
   String patchRollbackFile(Path file);

   @Message("Could not read %s")
   IllegalStateException patchCannotRead(Path patchesFile, @Cause IOException e);

   @Message("File '%s' already exists")
   FileAlreadyExistsException patchFileAlreadyExists(Path patch);

   @Message("At least three arguments are required: the patch file, the target server path and one or more source server paths")
   IllegalArgumentException patchCreateArgumentsRequired();

   @Message("You must specify the path to a patch archive")
   IllegalArgumentException patchArchiveArgumentRequired();

   @Message("Cannot create a patch from identical source and target server versions: %s")
   IllegalArgumentException patchServerAndTargetMustBeDifferent(String version);

   @Message("The patch archive appears to have a corrupt entry for: %s")
   String patchCorruptArchive(PatchOperation operation);

   @Message("Downloaded file '%s'")
   String downloadedFile(String filename);

   @Message(value = "Specify a username: ")
   String userToolUsername();

   @Message(value = "Set a password for the user: ")
   String userToolPassword();

   @Message(value = "Confirm the password for the user: ")
   String userToolPasswordConfirm();

   @Message(value = "User `%s` already exists")
   IllegalStateException userToolUserExists(String username);

   @Message(value = "Cannot add user `%s` without a password.")
   String userToolNoPassword(String username);

   @Message("Error accessing file '%s'")
   RuntimeException userToolIOError(Path path, @Cause IOException e);

   @Message("Unkown password encryption algorithm: '%s'")
   IllegalArgumentException userToolUnknownAlgorithm(String algorithm);

   @Message(value = "User `%s` does not exist")
   IllegalArgumentException userToolNoSuchUser(String username);

   @Message(value = "{ username: \"%s\", realm: \"%s\", groups = %s }")
   String userDescribe(String username, String realm, String[] userGroups);

   @Message(value = "Invalid Unicode sequence '%s'")
   IOException invalidUnicodeSequence(String sequence, @Cause NoSuchElementException e);

   @Message(value = "Attempt to use %s passwords, but only %s passwords are allowed")
   IllegalArgumentException userToolIncompatibleEncrypyion(UserTool.Encryption encryption1, UserTool.Encryption encryption2);

   @Message(value = "Attempted to use a different realm '%s' than the already existing one '%s'")
   IllegalArgumentException userToolWrongRealm(String realm1, String realm2);

   @Message(value = "Unable to load CLI configuration from `%s`. Using defaults.")
   String configLoadFailed(String path);

   @Message(value = "Unable to store CLI configuration to '%s'.")
   String configStoreFailed(String path);

   @Message(value = "Wrong argument count: %d.")
   IllegalArgumentException wrongArgumentCount(int size);


   @Message(value = "Backup path on the server must be absolute")
   IllegalArgumentException backupAbsolutePathRequired();

   @Message("No services found")
   NoSuchElementException noServicesFound();

   @Message("The service must be specified")
   IllegalStateException specifyService();

   @Message("The service '%s' is of the wrong type")
   IllegalArgumentException wrongServiceType(String serviceName);

   @Message("Cannot find service '%s'")
   IllegalArgumentException noSuchService(String serviceName);

   @Message("Cannot find generated secrets for service '%s'")
   IllegalStateException noGeneratedSecret(String serviceName);

   @Message("A namespace was not specified and a default has not been set")
   IllegalStateException noDefaultNamespace();

   @Message(value = "Enter the password for the credential keystore: ")
   String credentialToolPassword();

   @Message(value = "Confirm the password for the credential store: ")
   String credentialToolPasswordConfirm();

   @Message(value = "Set a credential for the alias: ")
   String credentialToolCredential();

   @Message(value = "Confirm the credential: ")
   String credentialToolCredentialConfirm();
}
