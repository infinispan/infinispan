package org.infinispan.server.server.filesystem;

import java.io.File;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Path {
   private final String name;
   private final File path;

   public Path(String name, File path) {
      this.name = name;
      this.path = path;
   }

   public String getName() {
      return name;
   }


   public File getPath() {
      return path;
   }


   public String toString() {
      return "Path{" +
            "name='" + name + '\'' +
            ", path=" + path +
            '}';
   }
}
