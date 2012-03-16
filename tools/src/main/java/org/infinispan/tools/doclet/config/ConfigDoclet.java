/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tools.doclet.config;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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
   static String header, footer, encoding, title, bottom, cp;

   public static boolean start(RootDoc root) throws IOException {

      System.out.println("STARTING CONFIG DOCLET");


      ConfigHtmlGenerator generator = new ConfigHtmlGenerator(encoding, title(), bottom, footer, header,
                                                        "Infinispan configuration options", Arrays.asList("Configuration", "Infinispan",
                                                                                                          "Data Grids", "Documentation", "Reference", "MBeans"), cp);
      generator.setRootDoc(root);

      generator.generateHtml(outputDirectory + File.separator + "config.html", "stylesheet2.css");

      System.out.println("FINISHING CONFIG DOCLET");

      return true;
   }

   private static String title() {
      String s = "Configuration options";
      if (title == null || title.length() == 0)
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
