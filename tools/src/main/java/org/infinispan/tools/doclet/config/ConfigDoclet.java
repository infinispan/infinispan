package org.infinispan.tools.doclet.config;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.formats.html.ConfigurationImpl;
import org.infinispan.tools.doclet.html.HtmlGenerator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * A Doclet that generates configuration guide for Infinispan
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@SuppressWarnings("restriction")
public class ConfigDoclet {
   static String outputDirectory = ".";
   static String header, footer, encoding, title, bottom, cp;

   public static boolean start(RootDoc root) throws IOException {

      System.out.println("STARTING CONFIG DOCLET");


      HtmlGenerator generator = new ConfigHtmlGenerator(encoding, title(), bottom, footer, header,
                                                        "Infinispan configuration options", Arrays.asList("Configuration", "Infinispan",
                                                                                                          "Data Grids", "Documentation", "Reference", "MBeans"), cp);

      generator.generateHtml(outputDirectory + File.separator + "config.html");

      System.out.println("FINISHING CONFIG DOCLET");

      return true;
   }

   private static String title() {
      String s = "Configuration options";
      if (title == null || title.equals(""))
         return s;
      else {
         s += " (" + title + ")";
         return s;
      }
   }

   public static int optionLength(String option) {
      return (ConfigurationImpl.getInstance()).optionLength(option);
   }

   public static boolean validOptions(String options[][], DocErrorReporter reporter) {
      for (String[] option : options) {
         if (option[0].equals("-d"))
            outputDirectory = option[1];
         else if (option[0].equals("-encoding"))
            encoding = option[1];
         else if (option[0].equals("-bottom"))
            bottom = option[1];
         else if (option[0].equals("-footer"))
            footer = option[1];
         else if (option[0].equals("-header"))
            header = option[1];
         else if (option[0].equals("-doctitle"))
            title = option[1];
         else if (option[0].equals("-classpath"))
            cp = option[1];
      }
      return (ConfigurationImpl.getInstance()).validOptions(options, reporter);
   }
}
