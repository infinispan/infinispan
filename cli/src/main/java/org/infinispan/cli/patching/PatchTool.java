package org.infinispan.cli.patching;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.cli.logging.Messages.MSG;
import static org.infinispan.cli.util.Utils.sha256;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;

/**
 * Creates/installs/removes patches
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class PatchTool {
   public static final String PATCHES_DIR = ".patches";
   public static final String PATCHES_FILE = "patches.json";
   private final PrintStream out;
   private final PrintStream err;

   public PatchTool(PrintStream out, PrintStream err) {
      this.out = out;
      this.err = err;
   }

   public void createPatch(String qualifier, Path patch, Path target, Path... sources) throws IOException {
      // Obtain version information
      Version targetVersion = getVersion(target);
      // Build a list of files in the target
      Map<Path, ServerFile> targetFiles = getServerFiles(target);

      // Create the patch zip file
      try (FileSystem zipfs = getPatchFile(patch, true)) {
         for (Path source : sources) {
            createSinglePatch(qualifier, source, target, targetVersion, targetFiles, zipfs);
         }
      }
   }

   public void describePatch(Path patch, boolean verbose) throws IOException {
      try (FileSystem zipfs = getPatchFile(patch)) {
         getPatchInfos(zipfs).forEach(patchInfo -> {
                  out.println(patchInfo);
                  if (verbose) {
                     patchInfo.getOperations().forEach(op -> out.println("  " + op));
                  }
               }
         );
      }
   }

   public void listPatches(Path target, boolean verbose) {
      List<PatchInfo> installedPatches = getInstalledPatches(target);
      if (installedPatches.isEmpty()) {
         out.println(MSG.patchNoPatchesInstalled());
      } else {
         for (PatchInfo patchInfo : installedPatches) {
            out.println(MSG.patchInfo(patchInfo));
            if (verbose) {
               patchInfo.getOperations().forEach(op -> out.println("  " + op));
            }
         }
      }
   }

   public void installPatch(Path patch, Path target, boolean dryRun) throws IOException {
      // Obtain the target version
      Version targetVersion = getVersion(target);
      String version = targetVersion.brandVersion();
      String brandName = targetVersion.brandName();
      List<PatchInfo> installedPatches = getInstalledPatches(target);
      // Open the patch file
      try (FileSystem zipfs = getPatchFile(patch)) {
         // Iterate the patch json files to find one that matches our version as a source
         PatchInfo patchInfo = getPatchInfos(zipfs).stream()
               .filter(info -> brandName.equals(info.getBrandName()) && version.equals(info.getSourceVersion()))
               .findFirst().orElseThrow(() -> {
                  throw MSG.patchCannotApply(brandName, version);
               });
         // Validate the SHAs of the existing files against the source ones in the patch
         List<PatchOperation> operations = patchInfo.getOperations();

         // Collect all errors
         List<String> errors = new ArrayList<>();
         // Scan the patch to ensure its contents match with the list of operations
         for (PatchOperation operation : operations) {
            switch (operation.getAction()) {
               case ADD:
               case SOFT_REPLACE:
               case HARD_REPLACE:
               case UPGRADE:
                  String sha256 = sha256(zipfs.getPath(operation.getNewPath().toString()));
                  if (sha256 == null || !sha256.equals(operation.getNewDigest())) {
                     errors.add(MSG.patchCorruptArchive(operation));
                  }
            }
         }
         if (errors.size() > 0) {
            throw MSG.patchValidationErrors(String.join("\n", errors));
         }
         // Scan the server files to ensure that the patch can be installed
         for (PatchOperation operation : operations) {
            switch (operation.getAction()) {
               case ADD:
               case SOFT_REPLACE:
                  // Ignore adds and soft replaces
                  break;
               case REMOVE:
               case HARD_REPLACE:
               case UPGRADE:
                  String sha256 = sha256(target.resolve(operation.getPath()));
                  if (sha256 == null || !sha256.equals(operation.getDigest())) {
                     errors.add(MSG.patchShaMismatch(operation.getPath(), operation.getDigest(), sha256));
                  }
                  break;
            }
         }
         if (errors.size() > 0) {
            throw MSG.patchValidationErrors(String.join("\n", errors));
         }
         // We're good to go, backup the files being removed / replaced
         Path backup = getBackupPath(target, patchInfo);
         Files.createDirectories(backup);
         for (PatchOperation operation : operations) {
            switch (operation.getAction()) {
               case ADD:
                  // Ignore adds
                  break;
               case SOFT_REPLACE:
                  // We backup only if the checksum matches (which means we will be replacing a distribution file)
                  String sha256 = sha256(target.resolve(operation.getPath()));
                  if (sha256 == null || !sha256.equals(operation.getDigest())) {
                     break;
                  }
               case REMOVE:
               case HARD_REPLACE:
               case UPGRADE:
                  Path file = backup.resolve(operation.getPath());
                  println(dryRun, MSG.patchBackup(target.resolve(operation.getPath()), file));
                  if (!dryRun) {
                     Files.createDirectories(file.getParent());
                     Files.move(target.resolve(operation.getPath()), file);
                  }
                  break;
            }
         }
         // Now perform the actual operations
         for (PatchOperation operation : operations) {
            switch (operation.getAction()) {
               case REMOVE:
                  // Do nothing, the file has already been removed as part of the backup
                  break;
               case SOFT_REPLACE:
                  String sha256 = sha256(target.resolve(operation.getPath()));
                  if (sha256 == null || sha256.equals(operation.getDigest())) {
                     if (!dryRun) {
                        Path file = Files.copy(zipfs.getPath(operation.getNewPath().toString()), target.resolve(operation.getNewPath()));
                        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString(operation.getNewPermissions()));
                     }
                  } else {
                     // We create a new file by appending the target version to the filename
                     if (!dryRun) {
                        Path file = target.resolve(operation.getNewPath());
                        file = file.getParent().resolve(file.getFileName().toString() + "-" + patchInfo.getTargetVersion());
                        Files.copy(zipfs.getPath(operation.getNewPath().toString()), file);
                        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString(operation.getNewPermissions()));
                     }
                  }
                  break;
               case ADD:
               case HARD_REPLACE:
               case UPGRADE:
                  if (!dryRun) {
                     Path file = target.resolve(operation.getNewPath());
                     if (file.getParent() != null) {
                        Files.createDirectories(file.getParent());
                     }
                     Files.copy(zipfs.getPath(operation.getNewPath().toString()), file, StandardCopyOption.REPLACE_EXISTING);
                     Files.setPosixFilePermissions(file, PosixFilePermissions.fromString(operation.getNewPermissions()));
                  }
                  break;
            }
         }
         patchInfo.setInstallationDate(new Date());
         if (!dryRun) {
            installedPatches.add(patchInfo);
            writeInstalledPatches(target, installedPatches);
         }
         println(dryRun, MSG.patchInfo(patchInfo));
      }
   }

   public void rollbackPatch(Path target, boolean dryRun) throws IOException {
      List<PatchInfo> installedPatches = getInstalledPatches(target);
      if (installedPatches.isEmpty()) {
         throw MSG.patchNoPatchesInstalledToRollback();
      }
      PatchInfo patchInfo = installedPatches.remove(installedPatches.size() - 1);
      Path backup = getBackupPath(target, patchInfo);
      for (PatchOperation operation : patchInfo.getOperations()) {
         switch (operation.getAction()) {
            case ADD: {
               Path file = target.resolve(operation.getNewPath());
               // Remove any added files
               println(dryRun, MSG.patchRollbackFile(file));
               if (!dryRun) {
                  Files.delete(file);
               }
            }
            break;
            case SOFT_REPLACE: {
               // We only restore if the file hasn't been changed
               Path file = target.resolve(operation.getPath());
               String sha256 = sha256(file);
               if (sha256 != null && sha256.equals(operation.getNewDigest())) {
                  println(dryRun, MSG.patchRollbackFile(file));
                  if (!dryRun) {
                     Files.move(backup.resolve(operation.getPath()), target.resolve(operation.getPath()), StandardCopyOption.REPLACE_EXISTING);
                  }
               }
               // We might have created a side-file, remove it
               file = target.resolve(operation.getNewPath());
               file = file.getParent().resolve(file.getFileName().toString() + "-" + patchInfo.getTargetVersion());
               if (Files.exists(file)) {
                  println(dryRun, MSG.patchRollbackFile(file));
                  if (!dryRun) {
                     Files.delete(file);
                  }
               }
               break;
            }
            case UPGRADE: {
               Path file = target.resolve(operation.getNewPath());
               if (!dryRun) {
                  Files.delete(file);
               }
            }
            // Fall through to add the backed-up file
            case REMOVE:
            case HARD_REPLACE: {
               Path file = target.resolve(operation.getPath());
               println(dryRun, MSG.patchRollbackFile(file));
               if (!dryRun) {
                  Files.move(backup.resolve(operation.getPath()), target.resolve(operation.getPath()), StandardCopyOption.REPLACE_EXISTING);
               }
            }
            break;
         }
      }
      if (!dryRun) {
         writeInstalledPatches(target, installedPatches);
      }
      println(dryRun, MSG.patchRollback(patchInfo));
   }

   private void println(boolean dryRun, String msg) {
      if (dryRun) {
         out.print(MSG.patchDryRun());
      }
      out.println(msg);
   }

   private Path getBackupPath(Path target, PatchInfo patchInfo) {
      return target.resolve(PATCHES_DIR).resolve(patchInfo.getSourceVersion() + "_" + patchInfo.getTargetVersion());
   }

   private List<PatchInfo> getInstalledPatches(Path target) {
      Path patchesFile = target.resolve(PATCHES_DIR).resolve(PATCHES_FILE);
      try (InputStream is = Files.newInputStream(patchesFile, StandardOpenOption.READ)) {
         Json read = Json.read(Util.read(is));
         return read.asJsonList().stream().map(PatchInfo::fromJson).collect(Collectors.toList());
      } catch (NoSuchFileException e) {
         return new ArrayList<>();
      } catch (IOException e) {
         throw MSG.patchCannotRead(patchesFile, e);
      }
   }

   private void writeInstalledPatches(Path target, List<PatchInfo> patches) {
      try (OutputStream os = Files.newOutputStream(Files.createDirectories(target.resolve(PATCHES_DIR)).resolve(PATCHES_FILE), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
         String json = Json.make(patches).toString();
         os.write(json.getBytes(UTF_8));
      } catch (IOException e) {
         throw MSG.patchCannotWritePatchesFile(e);
      }
   }

   private void createSinglePatch(String qualifier, Path source, Path target, Version targetVersion, Map<Path, ServerFile> targetFiles, FileSystem zipfs) throws IOException {
      Version sourceVersion = getVersion(source);

      // Ensure that the brand name coincides
      String sourceBrand = sourceVersion.brandName();
      String targetBrand = targetVersion.brandName();
      if (!sourceBrand.equals(targetBrand)) {
         throw MSG.patchIncompatibleProduct(sourceBrand, targetBrand);
      }

      if (sourceVersion.brandVersion().equals(targetVersion.brandVersion())) {
         throw MSG.patchServerAndTargetMustBeDifferent(sourceVersion.brandVersion());
      }

      PatchInfo patchInfo = new PatchInfo(sourceBrand, sourceVersion.brandVersion(), targetVersion.brandVersion(), qualifier);

      // Build a list of files in the old version
      Map<Path, ServerFile> v1Files = getServerFiles(source);

      // Compare the two file lists, generating a list of upgrade instructions
      List<PatchOperation> operations = patchInfo.getOperations();
      v1Files.forEach((k1, v1File) -> {
         if (!targetFiles.containsKey(k1)) {
            operations.add(PatchOperation.remove(v1File.getVersionedPath(), v1File.getDigest(), v1File.getPermissions()));
         } else {
            ServerFile targetFile = targetFiles.get(k1);
            if (!v1File.getFilename().equals(targetFile.getFilename())) { // Different filename means upgrade
               operations.add(PatchOperation.upgrade(v1File.getVersionedPath(), v1File.getDigest(), v1File.getPermissions(), targetFile.getVersionedPath(), targetFile.getDigest(), targetFile.getPermissions()));
               addFileToZip(zipfs, target, targetFile);
            } else if (!v1File.getDigest().equals(targetFile.getDigest())) {
               // Check contents
               operations.add(PatchOperation.replace(targetFile.isSoft(), targetFile.getVersionedPath(), v1File.getDigest(), v1File.getPermissions(), targetFile.getDigest(), targetFile.getPermissions()));
               addFileToZip(zipfs, target, targetFile);
            }
         }
      });
      targetFiles.forEach((k2, targetFile) -> {
         if (!v1Files.containsKey(k2)) {
            operations.add(PatchOperation.add(targetFile.getVersionedPath(), targetFile.getDigest(), targetFile.getPermissions()));
            addFileToZip(zipfs, target, targetFile);
         }
      });

      // Write out the JSON patch file
      Path patchPath = zipfs.getPath("patch-" + patchInfo.getSourceVersion() + "_" + patchInfo.getTargetVersion() + ".json");
      try (OutputStream os = Files.newOutputStream(patchPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
         String json = patchInfo.toJson().toPrettyString();
         os.write(json.getBytes(UTF_8));
      }
   }

   private Map<Path, ServerFile> getServerFiles(Path base) throws IOException {
      Pattern IGNORE = Pattern.compile("^(\\.patches/|server/data/|server/log/|server/lib/).*$");
      Pattern SOFT_REPLACE_PATTERN = Pattern.compile("^server/conf/.*$");
      Map<Path, ServerFile> files = new TreeMap<>();
      Files.walkFileTree(base, new SimpleFileVisitor<Path>() {
         @Override
         public FileVisitResult visitFile(Path oPath, BasicFileAttributes attrs) {
            Path rPath = base.relativize(oPath);
            String rPathName = rPath.toString();
            if (!IGNORE.matcher(rPathName).matches()) {
               ServerFile file = new ServerFile(rPath, sha256(oPath), getPermissions(oPath), SOFT_REPLACE_PATTERN.matcher(rPathName).matches());
               files.put(file.getUnversionedPath(), file);
            }
            return FileVisitResult.CONTINUE;
         }
      });
      return files;
   }

   private static String getPermissions(Path path) {
      try {
         return PosixFilePermissions.toString(Files.getPosixFilePermissions(path));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private List<PatchInfo> getPatchInfos(FileSystem zipfs) throws IOException {
      List<Path> paths = Files.find(zipfs.getPath("/"), 1,
            (p, a) -> a.isRegularFile() && p.getFileName().toString().startsWith("patch-") && p.getFileName().toString().endsWith(".json")
      ).collect(Collectors.toList());
      List<PatchInfo> infos = new ArrayList<>(paths.size());
      for (Path path : paths) {
         try (InputStream is = Files.newInputStream(path, StandardOpenOption.READ)) {
            Json json = Json.read(Util.read(is));
            infos.add(PatchInfo.fromJson(json));
         }
      }
      return infos;
   }

   private Version getVersion(Path base) throws IOException {
      // Load the META-INF/infinispan-version.properties from the lib/infinispan-commons jar
      Path lib = base.resolve("lib");
      File[] commons = lib.toFile().listFiles((dir, name) -> name.startsWith("infinispan-commons-") && name.endsWith(".jar"));
      if (commons == null || commons.length != 1) {
         throw MSG.patchCannotFindCommons(lib);
      }
      URI jarUri = URI.create("jar:" + commons[0].toURI());
      try (FileSystem zipfs = FileSystems.newFileSystem(jarUri, Collections.emptyMap()); InputStream in = Files.newInputStream(zipfs.getPath("META-INF", "infinispan-version.properties"))) {
         return Version.from(in);
      }
   }


   private FileSystem getPatchFile(Path patch, boolean create) throws IOException {
      if (create && patch.toFile().exists()) {
         throw MSG.patchFileAlreadyExists(patch);
      }
      URI jarUri = URI.create("jar:" + patch.toUri());
      return FileSystems.newFileSystem(jarUri, create ? Collections.singletonMap("create", "true") : Collections.emptyMap());
   }

   private FileSystem getPatchFile(Path patch) throws IOException {
      return getPatchFile(patch, false);
   }

   private void addFileToZip(FileSystem zipfs, Path basePath, ServerFile file) {
      try {
         Path target = zipfs.getPath(file.getVersionedPath().toString());
         out.println(MSG.patchCreateAdd(target));
         if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
         }
         Files.copy(basePath.resolve(file.getVersionedPath()), target, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
         throw MSG.patchCreateError(e);
      }
   }
}
