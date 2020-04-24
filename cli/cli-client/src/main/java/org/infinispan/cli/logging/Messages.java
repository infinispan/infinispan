package org.infinispan.cli.logging;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import org.infinispan.cli.patching.PatchInfo;
import org.infinispan.cli.patching.PatchOperation;
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

   @Message("Downloaded report '%s'")
   String downloadedReport(String filename);
}
