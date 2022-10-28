package org.infinispan.cli.logging;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import org.aesh.command.CommandException;
import org.aesh.command.parser.RequiredOptionException;
import org.infinispan.cli.patching.PatchInfo;
import org.infinispan.cli.patching.PatchOperation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.user.UserTool;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * @since 10.0
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
   Messages MSG = org.jboss.logging.Messages.getBundle(Messages.class);
   Logger CLI = Logger.getLogger("CLI");

   @Message(value = "Username: ")
   String username();

   @Message(value = "Password: ")
   String password();

   @Message("Not Found: %s")
   IOException notFound(String s);

   @Message("The supplied credentials are invalid %s")
   AccessDeniedException unauthorized(String s);

   @Message("Error: %s")
   IOException error(String s);

   @Message("The user is not allowed to access the server resource: %s")
   AccessDeniedException forbidden(String s);

   @Message("Error while configuring SSL")
   String keyStoreError(@Cause Exception e);

   @Message("No such resource '%s'")
   IllegalArgumentException noSuchResource(String name);

   @Message("Command invoked from the wrong context")
   IllegalStateException illegalContext();

   @Message("Illegal arguments for command")
   IllegalArgumentException illegalCommandArguments();

   @Message("The options '%s' and '%s' are mutually exclusive")
   IllegalArgumentException mutuallyExclusiveOptions(String arg1, String arg2);

   @Message("One of the '%s' and '%s' options are required")
   RequiredOptionException requiresOneOf(String arg1, String arg2);

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

   @Message("Cannot find service '%s' in namespace '%s'")
   IllegalArgumentException noSuchService(String serviceName, String namespace);

   @Message("Cannot find or access generated secrets for service '%s'")
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

   @Message(value = "Filter rule '%s' is not in the format [ACCEPT|REJECT]/{CIDR}")
   IllegalArgumentException illegalFilterRule(String rule);

   @Message(value = "Error executing file: %s, line %d: '%s'")
   CommandException batchError(String file, int lineNumber, String line, @Cause Throwable t);

   @Message("Option '%s' requires option '%s'")
   RequiredOptionException requiresAllOf(String option1, String option2);

   @Message("The cache name is required")
   IllegalArgumentException missingCacheName();

   @Message("Could not determine catalog source")
   IllegalStateException noCatalog();

   @Message("Target namespaces must be specified when not installing globally")
   IllegalArgumentException noTargetNamespaces();

   @Message("Could not find a default operator namespace")
   IllegalStateException noDefaultOperatorNamespace();

   @Message("Kubernetes client is unavailable in this mode")
   IllegalStateException noKubernetes();

   @Message("Could not find an operator subscription in namespace '%s'")
   IllegalStateException noOperatorSubscription(String namespace);

   @Message("Expose type '%s' requires a port")
   IllegalArgumentException exposeTypeRequiresPort(String exposeType);

   @Message("Encryption type '%s' requires a secret name")
   IllegalArgumentException encryptionTypeRequiresSecret(String encryptionType);

   @Message("No running pods available in service %s")
   IllegalStateException noRunningPodsInService(String name);

   @Message("A username must be specified")
   IllegalArgumentException usernameRequired();

   @Message("Checksum for '%s' does not match. Supplied: %s Actual: %s")
   SecurityException checksumFailed(String path, String checksum, String computed);

   @Message("Checksum for '%s' verified")
   String checksumVerified(String path);

   @Message("Artifact '%s' not found")
   IllegalArgumentException artifactNotFound(String path);

   @Message("Retry download '%d/%d'")
   String retryDownload(int retry, int retries);

   @Message("The resource does not support the '%s' list format")
   IllegalArgumentException unsupportedListFormat(Resource.ListFormat format);
}
