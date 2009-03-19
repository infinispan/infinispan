/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.util;

import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Holds the logic of looking up a file, in the following sequence: <ol> <li> try to load it with the curent thread's
 * context ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if fails, try to load it as a file from the
 * disck </li> </ol>
 *
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class FileLookup {
   private static final Log log = LogFactory.getLog(FileLookup.class);

   /**
    * Looks up the file, see : {@link FileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
   public InputStream lookupFile(String filename) {
      InputStream is = getAsInputStreamFromClassLoader(filename);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debug("Unable to find configuration file " + filename + " in classpath; searching for this file on the filesystem instead.");
         try {
            is = new FileInputStream(filename);
         }
         catch (FileNotFoundException e) {
            return null;
         }
      }
      return is;
   }

   protected InputStream getAsInputStreamFromClassLoader(String filename) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      InputStream is = cl == null ? null : cl.getResourceAsStream(filename);
      if (is == null) {
         // check system class loaderold
         is = getClass().getClassLoader().getResourceAsStream(filename);
      }
      return is;
   }

   public URL lookupFileLocation(String filename) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL u = cl == null ? null : cl.getResource(filename);
      if (u == null) {
         // check system class loaderold
         u = getClass().getClassLoader().getResource(filename);
      }
      if (u == null) {
         File f = new File(filename);
         if (f.exists()) try {
            u = f.toURI().toURL();
         }
         catch (MalformedURLException e) {
            // what do we do here?
         }
      }
      return u;
   }
}
