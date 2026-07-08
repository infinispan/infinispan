package org.infinispan.cli.commands;

import static org.infinispan.cli.util.Utils.digest;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.io.FileResource;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.ExitCodeResultHandler;
import org.infinispan.cli.logging.Messages;
import org.infinispan.commons.maven.Artifact;
import org.infinispan.commons.maven.MavenSettings;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.kohsuke.MetaInfServices;

/**
 * @since 14.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "install", description = "Downloads and installs artifacts into the lib folder of the server.", resultHandler = ExitCodeResultHandler.class)
public class Install extends CliCommand {

   @Arguments(description = "Specifies one or more artifacts as URLs or Maven GAV coordinates.", required = true)
   List<String> artifacts;

   @Option(completer = FileOptionCompleter.class, description = "Sets the path to the server installation.", name = "server-home")
   FileResource serverHome;

   @Option(description = "Sets the server root directory relative to the server home.", name = "server-root", defaultValue = "server")
   String serverRoot = "server";

   @Option(description = "Sets the path to a Maven settings file that resolves Maven artifacts. Can be either a local path or a URL.", name = "maven-settings")
   String mavenSettings;


   @Option(description = "Overwrites artifacts in the lib folder. By default installation fails if artifacts already exist.", shortName = 'o', hasValue = false)
   boolean overwrite;

   @Option(description = "Show verbose information about installation progress.", shortName = 'v', hasValue = false)
   boolean verbose;

   @Option(description = "Forces download of artifacts, ignoring any previously cached versions.", shortName = 'f', hasValue = false)
   boolean force;

   @Option(description = "Number of download retries in case artifacts do not match the supplied checksums.", shortName = 'r', defaultValue = "0")
   int retries;

   @Option(description = "Deletes all contents from the lib directory before downloading artifacts.", hasValue = false)
   boolean clean;

   @Option(description = "Also installs dependencies declared by the artifact.", shortName = 'd', name = "with-dependencies", hasValue = false)
   boolean withDependencies;

   private String serverVersion;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      try {
         StandardCopyOption[] options = overwrite ? new StandardCopyOption[]{StandardCopyOption.REPLACE_EXISTING} : new StandardCopyOption[]{};
         MavenSettings.init(mavenSettings == null ? null : Artifact.fromString(mavenSettings).resolveArtifact());
         Path serverLib = CLI.getServerHome(serverHome).resolve(serverRoot).resolve("lib");
         if (clean) {
            if (verbose) {
               System.out.printf("Removing all files from %s%n", serverLib);
            }
            Util.recursiveFileRemove(serverLib);
            Files.createDirectories(serverLib);
         }
         for (String artifact : artifacts) {
            Path resolved = installArtifact(artifact, serverLib, options);
            if (withDependencies && resolved != null) {
               installDependencies(resolved, serverLib, options);
            }
         }
         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new CommandException(e);
      }
   }

   private Path installArtifact(String artifact, Path serverLib, StandardCopyOption[] options) throws IOException, CommandException {
      String[] parts = artifact.split("\\|");
      String path = parts[0];
      Path resolved = null;
      for (int retry = 0; retry <= retries; retry++) {
         resolved = Artifact.fromString(path, getServerVersion(serverLib)).verbose(verbose).force(retry != 0 || force).resolveArtifact();
         if (resolved == null) {
            throw Messages.MSG.artifactNotFound(path);
         }
         if (parts.length > 1) {
            String checksum = parts.length == 3 ? parts[2].toUpperCase(Locale.ROOT) : parts[1].toUpperCase(Locale.ROOT);
            String algorithm = parts.length == 3 ? parts[1].toUpperCase(Locale.ROOT) : "SHA-256";
            String computed = digest(resolved, algorithm);
            if (!computed.equals(checksum)) {
               if (retry < retries) {
                  if (verbose) {
                     System.err.printf("%s. %s%n", Messages.MSG.checksumFailed(path, checksum, computed).getMessage(), Messages.MSG.retryDownload(retry + 1, retries));
                  }
               } else {
                  throw Messages.MSG.checksumFailed(path, checksum, computed);
               }
            } else if (verbose) {
               System.out.println(Messages.MSG.checksumVerified(path));
               break;
            }
         } else {
            break;
         }
      }
      String resolvedFilename = resolved.getFileName().toString();
      if (resolvedFilename.endsWith(".zip")) {
         extractZip(resolved, serverLib, options);
      } else if (resolvedFilename.endsWith(".tgz") || resolvedFilename.endsWith(".tar.gz")) {
         extractTgz(resolved, serverLib, options);
      } else if (resolvedFilename.endsWith(".tar")) {
         extractTar(resolved, serverLib, options);
      } else {
         Files.copy(resolved, serverLib.resolve(resolved.getFileName()), options);
      }
      return resolved;
   }

   private void installDependencies(Path artifactJar, Path serverLib, StandardCopyOption[] options) throws IOException, CommandException {
      String filename = artifactJar.getFileName().toString();
      if (!filename.endsWith(".jar")) {
         return;
      }
      List<String> dependencies = readDependencies(artifactJar);
      for (String dep : dependencies) {
         String artifactId = extractArtifactId(dep);
         if (artifactId != null && hasExistingArtifact(serverLib, artifactId)) {
            if (verbose) {
               System.out.println(Messages.MSG.skippingExistingDependency(artifactId));
            }
            continue;
         }
         if (verbose) {
            System.out.println(Messages.MSG.installingDependency(dep));
         }
         installArtifact(dep, serverLib, options);
      }
   }

   private List<String> readDependencies(Path jar) throws IOException {
      URI jarUri = URI.create("jar:" + jar.toUri());
      try (FileSystem zipfs = FileSystems.newFileSystem(jarUri, Collections.emptyMap())) {
         Path depsPath = zipfs.getPath("META-INF", "infinispan-dependencies.txt");
         if (!Files.exists(depsPath)) {
            return Collections.emptyList();
         }
         List<String> deps = new ArrayList<>();
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(depsPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
               line = line.trim();
               if (!line.isEmpty()) {
                  deps.add(line);
               }
            }
         }
         return deps;
      }
   }

   private static String extractArtifactId(String gav) {
      String[] parts = gav.split(":");
      return parts.length >= 2 ? parts[1] : null;
   }

   private static boolean hasExistingArtifact(Path serverLib, String artifactId) throws IOException {
      if (!Files.isDirectory(serverLib)) {
         return false;
      }
      String prefix = artifactId + "-";
      try (var stream = Files.list(serverLib)) {
         return stream.anyMatch(p -> {
            String name = p.getFileName().toString();
            return name.startsWith(prefix) && name.endsWith(".jar");
         });
      }
   }

   private String getServerVersion(Path serverLib) {
      if (serverVersion != null) {
         return serverVersion;
      }
      if (!Files.isDirectory(serverLib)) {
         return null;
      }
      File[] commons = serverLib.toFile().listFiles((dir, name) -> name.startsWith("infinispan-commons-") && name.endsWith(".jar"));
      if (commons == null || commons.length != 1) {
         return null;
      }
      URI jarUri = URI.create("jar:" + commons[0].toURI());
      try (FileSystem zipfs = FileSystems.newFileSystem(jarUri, Collections.emptyMap());
           InputStream in = Files.newInputStream(zipfs.getPath("META-INF", "infinispan-version.properties"))) {
         serverVersion = Version.from(in).version();
         return serverVersion;
      } catch (IOException e) {
         return null;
      }
   }

   private static void extractZip(Path zip, Path dest, CopyOption... options) throws IOException {
      try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip))) {
         for (ZipEntry zipEntry = zis.getNextEntry(); zipEntry != null; zipEntry = zis.getNextEntry()) {
            Path entryPath = dest.resolve(zipEntry.getName());
            if (!entryPath.startsWith(dest)) {
               throw new IOException("Illegal relative path " + zipEntry.getName());
            }
            if (zipEntry.isDirectory()) {
               Files.createDirectories(entryPath);
            } else {
               Files.createDirectories(entryPath.getParent());
               Files.copy(zis, entryPath, options);
            }
            zis.closeEntry();
         }
      }
   }

   private static void extractTgz(Path tar, Path dest, CopyOption... options) throws IOException {
      try (TarArchiveInputStream tis = new TarArchiveInputStream(new GZIPInputStream(Files.newInputStream(tar)))) {
         extractTarEntries(dest, tis, options);
      }
   }

   private static void extractTar(Path tar, Path dest, CopyOption... options) throws IOException {
      try (TarArchiveInputStream tis = new TarArchiveInputStream(Files.newInputStream(tar))) {
         extractTarEntries(dest, tis, options);
      }
   }

   private static void extractTarEntries(Path dest, TarArchiveInputStream tis, CopyOption... options) throws IOException {
      for (TarArchiveEntry tarEntry = tis.getNextTarEntry(); tarEntry != null; tarEntry = tis.getNextTarEntry()) {
         Path entryPath = dest.resolve(tarEntry.getName());
         if (!entryPath.startsWith(dest)) {
            throw new IOException("Illegal relative path " + tarEntry.getName());
         }
         if (tarEntry.isDirectory()) {
            Files.createDirectories(entryPath);
         } else {
            Files.createDirectories(entryPath.getParent());
            Files.copy(tis, entryPath, options);
         }
      }
   }
}
