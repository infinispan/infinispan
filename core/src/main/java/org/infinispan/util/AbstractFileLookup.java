/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;

import org.infinispan.util.FileLookupFactory.DefaultFileLookup;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;

public abstract class AbstractFileLookup implements FileLookup {

   private static final BasicLogger log = BasicLogFactory.getLog(FileLookup.class);

   public AbstractFileLookup() {
      super();
   }

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
   @Override
   public InputStream lookupFile(String filename, ClassLoader cl) {
      InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename, cl);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debugf("Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename);
         try {
            is = new FileInputStream(filename);
         }
         catch (FileNotFoundException e) {
            return null;
         }
      }
      return is;
   }

   protected abstract InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader cl);

   /**
    * Looks up the file, see : {@link DefaultFileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   @Override
   public InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException {
      InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename, cl);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debugf("Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename);
         return new FileInputStream(filename);
      }
      return is;
   }

   @Override
   public URL lookupFileLocation(String filename, ClassLoader cl) {
      URL u = getAsURLFromClassLoader(filename, cl);
   
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

   protected abstract URL getAsURLFromClassLoader(String filename, ClassLoader cl);

   @Override
   public Collection<URL> lookupFileLocations(String filename, ClassLoader cl) throws IOException {
      Collection<URL> u = getAsURLsFromClassLoader(filename, cl);
   
         File f = new File(filename);
         if (f.exists()) try {
            u.add(f.toURI().toURL());
         }
         catch (MalformedURLException e) {
            // what do we do here?
         }
      return u;
   }

   protected abstract Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader cl) throws IOException;

}