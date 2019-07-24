package org.infinispan.server.loader;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashSet;
import java.util.Set;

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

   public static final String DEFAULT_SERVER_ROOT_DIR = "server";

   public static void main(String[] args) {
      if (args.length == 0) {
         System.err.println("You must specify a classname to launch");
      }
      String home = System.getProperty(INFINISPAN_SERVER_HOME_PATH, System.getProperty("user.dir"));
      ClassLoader bootClassLoader = Loader.class.getClassLoader();
      ClassLoader serverClassLoader = classLoaderFromPath(Paths.get(home, "lib"), bootClassLoader);
      String root = System.getProperty(INFINISPAN_SERVER_ROOT_PATH, Paths.get(home, DEFAULT_SERVER_ROOT_DIR).toString());
      ClassLoader rootClassLoader = classLoaderFromPath(Paths.get(root, "lib"), serverClassLoader);
      Thread.currentThread().setContextClassLoader(rootClassLoader);
      try {
         Class<?> mainClass = rootClassLoader.loadClass(args[0]);
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
         Set<URL> jars = new LinkedHashSet<>();
         Files.walk(path)
               .filter(f -> f.toString().endsWith(".jar"))
               .forEach(jar -> {
                  try {
                     jars.add(jar.toUri().toURL());
                  } catch (MalformedURLException e) {
                  }
               });
         final URL[] array = jars.toArray(new URL[jars.size()]);
         return AccessController.doPrivileged(
               (PrivilegedAction<URLClassLoader>) () -> {
                  if (parent == null)
                     return new URLClassLoader(array);
                  else
                     return new URLClassLoader(array, parent);
               });
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
