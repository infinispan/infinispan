package org.infinispan.cli.patching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Properties;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class PatchToolTest {

   @Test
   public void testPatchToolCreate() throws IOException {
      Path tmp = Paths.get(CommonsTestingUtil.tmpDirectory(PatchToolTest.class));
      Util.recursiveFileRemove(tmp.toFile());
      Files.createDirectories(tmp);
      Util.recursiveDirectoryCopy(new File("target/test-classes/patch").toPath(), tmp);

      // Create the infinispan-commons jars that identify a server's version
      Path v1 = tmp.resolve("v1");
      createFakeInfinispanCommons(v1, "Infinispan", "1.0.0.Final");
      Path v2 = tmp.resolve("v2");
      createFakeInfinispanCommons(v2, "Infinispan", "1.0.1.Final");
      Path v3 = tmp.resolve("v3");
      createFakeInfinispanCommons(v3, "Infinispan", "1.1.0.Final");

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ByteArrayOutputStream err = new ByteArrayOutputStream();

      PatchTool patchTool = new PatchTool(new PrintStream(out), new PrintStream(err));

      // List the installed patches on v1
      patchTool.listPatches(v1, false);
      assertContains(out, "No patches installed");
      assertEmpty(err);
      out.reset();

      // Create a patch zip that can patch v1 and v2 to v3
      Path patch = Paths.get("target/patch.zip");
      patch.toFile().delete();
      patchTool.createPatch("", patch, v3, v1, v2);
      assertContains(out, "Adding ");
      assertEmpty(err);
      out.reset();

      // Attempting to create the patch file again should fail
      Exceptions.expectException(FileAlreadyExistsException.class, () -> patchTool.createPatch("", patch, v3, v1, v2));

      // Ensure the zip file does not contain the .patches directory IGNOREME.txt files
      try (FileSystem zipfs  = FileSystems.newFileSystem(URI.create("jar:" + patch.toUri().toString()), Collections.emptyMap())) {
         Path root = zipfs.getRootDirectories().iterator().next();
         Files.walkFileTree(root, new SimpleFileVisitor<Path>()  {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
               assertNotEquals("/.patches", dir.toString());
               return super.preVisitDirectory(dir, attrs);
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)  {
               assertNotEquals("IGNOREME.txt", file.getFileName().toString());
               return FileVisitResult.CONTINUE;
            }
         });
      }

      // Describe the patches installed in the zip
      patchTool.describePatch(patch, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.1.Final");
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.0.Final");
      assertEmpty(err);
      out.reset();

      // Install the patch on v1
      patchTool.installPatch(patch, v1, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.0.Final");
      assertEmpty(err);
      out.reset();

      // List the patches installed on v1
      patchTool.listPatches(v1, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.0.Final");
      assertEmpty(err);
      out.reset();

      // Install the patch on v2
      patchTool.installPatch(patch, v2, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.1.Final");
      assertEmpty(err);
      out.reset();

      // List the patches installed on v2
      patchTool.listPatches(v2, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.1.Final");
      assertEmpty(err);
      out.reset();

      // Rollback v1
      patchTool.rollbackPatch(v1, false);
      assertContains(out, "Rolled back patch Infinispan patch target=1.1.0.Final source=1.0.0.Final");
      assertEmpty(err);
      out.reset();

      // List the patches installed on v1
      patchTool.listPatches(v1, false);
      assertContains(out, "No patches installed");
      assertEmpty(err);
      out.reset();

      // Alter a configuration file on v1
      Path v1config = v1.resolve("server").resolve("conf").resolve("infinispan.xml");
      try(BufferedWriter w = new BufferedWriter(new FileWriter(v1config.toFile(), true))) {
         w.newLine();
         w.write("<!-- Some modification -->");
      }
      String v1sha256 = PatchTool.sha256(v1config);

      // Install the patch on v1
      patchTool.installPatch(patch, v1, false);
      assertContains(out, "Infinispan patch target=1.1.0.Final source=1.0.0.Final");
      assertEmpty(err);
      out.reset();

      // Ensure that the file has not been replaced
      assertEquals("Expecting SHA-256 of " + v1config +" to stay the same", v1sha256, PatchTool.sha256(v1config));
      // And that there is a new file next to it
      assertTrue(v1.resolve("server").resolve("conf").resolve("infinispan.xml-1.1.0.Final").toFile().exists());
   }

   private void assertContains(ByteArrayOutputStream out, String contains) {
      String s = out.toString();
      assertTrue(s.contains(contains));
   }

   private void assertEmpty(ByteArrayOutputStream out) {
      assertEquals("", out.toString());
   }

   private void createFakeInfinispanCommons(Path base, String brandName, String version) throws IOException {
      Path jar = base.resolve("lib").resolve("infinispan-commons-" + version + ".jar");
      Files.createDirectories(jar.getParent());
      URI jarUri = URI.create("jar:" + jar.toUri().toString());
      try (FileSystem zipfs = FileSystems.newFileSystem(jarUri, Collections.singletonMap("create", "true"))) {
         Path propsPath = zipfs.getPath("META-INF", "infinispan-version.properties");
         Files.createDirectories(propsPath.getParent());
         OutputStream os = Files.newOutputStream(propsPath, StandardOpenOption.CREATE);
         Properties properties = new Properties();
         properties.put("infinispan.version", version);
         properties.put("infinispan.brand.name", brandName);
         properties.store(os, null);
         os.close();
      }
   }
}
