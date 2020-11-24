package org.infinispan.server.loader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class LoaderTest {
   private static Path lib1;
   private static Path lib2;
   private static Path lib3;

   @BeforeClass
   public static void prepareLibs() throws IOException {
      Path path = Paths.get(System.getProperty("build.directory"), "test-classes", "server");
      lib1 = path.resolve("lib1");
      lib2 = path.resolve("lib2");
      lib3 = Files.createDirectories(lib2.resolve("lib3"));
      ShrinkWrap.create(JavaArchive.class,"two.jar")
            .addClasses(TwoClass.class).as(ZipExporter.class).exportTo(lib2.resolve("two.jar").toFile(), true);
      ShrinkWrap.create(JavaArchive.class,"three.jar")
            .addClasses(ThreeClass.class).as(ZipExporter.class).exportTo(lib3.resolve("three.jar").toFile(), true);
   }

   @Test
   public void testLoaderViaSystemProperty() {
      Properties properties = new Properties();
      properties.put(Loader.INFINISPAN_SERVER_LIB_PATH, String.join(File.pathSeparator, lib1.toString(), lib2.toString(), lib3.toString()));
      Loader.run(new String[]{LoaderTest.class.getName()}, properties);
   }

   @Test
   public void testLoaderViaPropertyFile() throws IOException {
      Properties properties = new Properties();
      properties.put(Loader.INFINISPAN_SERVER_LIB_PATH, String.join(File.pathSeparator, lib1.toString(), lib2.toString(), lib3.toString()));
      Path propertyFile = Paths.get(System.getProperty("build.directory"), "loader.properties");
      try (Writer w = Files.newBufferedWriter(propertyFile)) {
         properties.store(w, null);
      }
      Loader.run(new String[]{LoaderTest.class.getName(), "-P", propertyFile.toAbsolutePath().toString()}, new Properties());
   }

   public static void main(String[] args) {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      assertTrue(contextClassLoader instanceof URLClassLoader);
      try (InputStream is = contextClassLoader.getResourceAsStream("test.properties")) {
         Properties properties = new Properties();
         properties.load(is);
         assertEquals("v", properties.getProperty("k"));
      } catch (IOException e) {
         fail(e.getMessage());
      }
      try {
         contextClassLoader.loadClass("org.infinispan.server.loader.TwoClass");
         contextClassLoader.loadClass("org.infinispan.server.loader.ThreeClass");
      } catch (ClassNotFoundException e) {
         fail(e.getMessage());
      }
   }

}
