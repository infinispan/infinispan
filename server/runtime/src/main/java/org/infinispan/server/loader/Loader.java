package org.infinispan.server.loader;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Generic loader which constructs a classloader from all the jars found in some known locations and invokes the main
 * method on a specified class. This allows us to avoid the construction of huge classpaths in the shell scripts that
 * launch the server.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Loader {
   /**
    * Property name indicating the path to the server installation. If unspecified, the current working directory will
    * be used
    */
   public static final String INFINISPAN_SERVER_HOME_PATH = "infinispan.server.home.path";
   /**
    * Property name indicating the path to the root of a server instance. If unspecified, defaults to the <i>server</i>
    * directory under the server home.
    */
   public static final String INFINISPAN_SERVER_ROOT_PATH = "infinispan.server.root.path";
   /**
    * Property name indicating the paths to the server lib directories. If unspecified, defaults to the <i>lib</i>
    * directory under the server root.
    */
   public static final String INFINISPAN_SERVER_LIB_PATH = "infinispan.server.lib.path";

   public static final String DEFAULT_SERVER_ROOT_DIR = "server";

   public static void main(String[] args) {
      run(args, System.getProperties());
   }

   public static void run(String[] args, Properties properties) {
      if (args.length == 0) {
         System.err.println("You must specify a classname to launch");
      }
      // Scan the arguments looking for -s, --server-root=, -P, --properties=
      String root = null;
      String propertyFile = null;
      for (int i = 0; i < args.length; i++) {
         if ("-s".equals(args[i]) && i < args.length - 1) {
            root = args[i + 1];
            break;
         } else if (args[i].startsWith("--server-root=")) {
            root = args[i].substring(args[i].indexOf('=') + 1);
            break;
         } else if ("-P".equals(args[i]) && i < args.length - 1) {
            propertyFile = args[i + 1];
            break;
         } else if (args[i].startsWith("--properties=")) {
            propertyFile = args[i].substring(args[i].indexOf('=') + 1);
            break;
         }
      }
      if (propertyFile != null) {
         try (Reader r = Files.newBufferedReader(Paths.get(propertyFile))) {
            Properties loaded = new Properties();
            loaded.load(r);
            loaded.forEach(properties::putIfAbsent);
         } catch (IOException e) {
            throw new IllegalArgumentException(e);
         }
      }
      String home = properties.getProperty(INFINISPAN_SERVER_HOME_PATH, properties.getProperty("user.dir"));
      ClassLoader bootClassLoader = Loader.class.getClassLoader();
      ClassLoader serverClassLoader = classLoaderFromPath(Paths.get(home, "lib"), bootClassLoader);

      if (root == null) {
         root = properties.getProperty(INFINISPAN_SERVER_ROOT_PATH, Paths.get(home, DEFAULT_SERVER_ROOT_DIR).toString());
      }
      String lib = properties.getProperty(INFINISPAN_SERVER_LIB_PATH);
      if (lib != null) {
         for (String item : lib.split(File.pathSeparator)) {
            serverClassLoader = classLoaderFromPath(Paths.get(item), serverClassLoader);
         }
      } else {
         serverClassLoader = classLoaderFromPath(Paths.get(root, "lib"), serverClassLoader);
      }
      Thread.currentThread().setContextClassLoader(serverClassLoader);
      try {
         Class<?> mainClass = serverClassLoader.loadClass(args[0]);
         Method mainMethod = mainClass.getMethod("main", String[].class);
         String[] mainArgs = new String[args.length - 1];
         System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);
         mainMethod.invoke(null, (Object) mainArgs);
      } catch (Exception e) {
         System.err.println(e.getMessage());
         e.printStackTrace(System.err);
      }
   }

   public static ClassLoader classLoaderFromPath(Path path, ClassLoader parent) {
      try {
         if (!Files.exists(path)) {
            return parent;
         }
         Map<String, URL> urls = new LinkedHashMap<>();

         Files.walkFileTree(path, Collections.singleton(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path p, BasicFileAttributes attrs) throws IOException {
               urls.put(p.toString(), p.toUri().toURL());
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path p, BasicFileAttributes attrs) throws IOException {
               if (p.toString().endsWith(".jar")) {
                  String artifact = extractArtifactName(p.getFileName().toString());
                  if (urls.containsKey(artifact)) {
                     throw new IllegalArgumentException("Duplicate JARs:\n" + p.toAbsolutePath().normalize() + "\n" + urls.get(artifact));
                  } else {
                     urls.put(artifact, p.toUri().toURL());
                  }
               }
               return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path p, IOException exc) throws IOException {
               return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path p, IOException exc) throws IOException {
               return FileVisitResult.CONTINUE;
            }
         });
         final URL[] array = urls.values().toArray(new URL[urls.size()]);
         if (parent == null)
            return new URLClassLoader(array);
         else
            return new URLClassLoader(array, parent);
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private static String extractArtifactName(String filename) {
      int l = filename.length();
      for (int i = 0; i < l; i++) {
         char c = filename.charAt(i);
         if (c == '-' && i < l - 1) {
            c = filename.charAt(i + 1);
            if (c >= '0' && c <= '9') {
               return filename.substring(0, i);
            }
         }
      }
      // Could not obtain an artifact
      return filename;
   }
}
