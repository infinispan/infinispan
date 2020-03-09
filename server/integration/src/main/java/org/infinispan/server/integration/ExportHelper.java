package org.infinispan.server.integration;

import java.io.File;

import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;

public class ExportHelper {

   public static void export(String name, Archive archive) {

      String target = System.getProperty("project.build.directory");
      File file = new File(target, name);
      if (file.exists()) {
         file.delete();
      }
      archive.as(ZipExporter.class).exportTo(file, true);
   }
}
