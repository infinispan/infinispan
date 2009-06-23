package org.infinispan.tools.doclet.jmx;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.infinispan.tools.doclet.html.ConfigHtmlGenerator;
import org.infinispan.tools.doclet.html.HtmlGenerator;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.formats.html.ConfigurationImpl;

/**
 * A Doclet that generates configuration guide for Infinispan
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@SuppressWarnings("restriction")
public class ConfigDoclet {
   static String outputDirectory = ".";
   static String header, footer, encoding, title, bottom;

   public static boolean start(RootDoc root) throws IOException {

      HtmlGenerator generator = new ConfigHtmlGenerator(encoding, title(), bottom, footer, header,
               "Infinispan configuration options", Arrays.asList("Configuration", "Infinispan",
                        "Data Grids", "Documentation", "Reference", "MBeans"));

      generator.generateHtml(outputDirectory + File.separator + "config.html");

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
         // System.out.println("  >> Option " + Arrays.toString(option));
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
      }
      return (ConfigurationImpl.getInstance()).validOptions(options, reporter);
   }

   public static void main(String[] args) throws IOException {
      HtmlGenerator generator = new ConfigHtmlGenerator(encoding, title(), bottom, footer, header,
               "Infinispan configuration options", Arrays.asList("Configuration", "Infinispan",
                        "Data Grids", "Documentation", "Reference", "MBeans"));

      generator.generateHtml(outputDirectory + File.separator + "config.html");
   }
}
