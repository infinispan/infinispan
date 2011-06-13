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
package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

/**
 * Holds the logic of looking up a file, in the following sequence: <ol> <li> try to load it with the current thread's
 * context ClassLoader</li> <li> if fails, the system ClassLoader</li> <li> if fails, try to load it as a file from the
 * disk </li> </ol>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class FileLookup {
   private static final Log log = LogFactory.getLog(FileLookup.class);

   /**
    * Looks up the file, see : {@link FileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    */
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

   /**
    * Looks up the file, see : {@link FileLookup}.
    *
    * @param filename might be the name of the file (too look it up in the class path) or an url to a file.
    * @return an input stream to the file or null if nothing found through all lookup steps.
    * @throws FileNotFoundException if file cannot be found
    */
   public InputStream lookupFileStrict(String filename, ClassLoader cl) throws FileNotFoundException {
      InputStream is = filename == null || filename.length() == 0 ? null : getAsInputStreamFromClassLoader(filename, cl);
      if (is == null) {
         if (log.isDebugEnabled())
            log.debugf("Unable to find file %s in classpath; searching for this file on the filesystem instead.", filename);
         return new FileInputStream(filename);
      }
      return is;
   }


   protected InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader appClassLoader) {
      
      ClassLoader[] cls = Util.getClassLoaders(appClassLoader);
      for (ClassLoader cl : cls)  {
         if (cl == null)
            continue;
         try {
            return cl.getResourceAsStream(filename);
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return null;
   }
   
   protected URL getAsURLFromClassLoader(String filename, ClassLoader userClassLoader) {
      
      ClassLoader[] cls = new ClassLoader[] {
            userClassLoader,  // User defined classes
            Util.class.getClassLoader(), // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader() // Used when load time instrumentation is in effect
            };


      for (ClassLoader cl : cls)  {
         if (cl == null)
            continue;

         try {
            return cl.getResource(filename);
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return null;
   }
   
   protected Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {
      
      ClassLoader[] cls = new ClassLoader[] {
            userClassLoader,  // User defined classes
            Util.class.getClassLoader(), // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader() // Used when load time instrumentation is in effect
            };
      
      Collection<URL> urls = new HashSet<URL>();
      for (ClassLoader cl : cls)  {
         if (cl == null)
            continue;
         try {
            urls.addAll(new EnumerationList<URL>(cl.getResources(filename)));
         } catch (RuntimeException e) {
            // Ignore this as the classloader may throw exceptions for a valid path on Windows
         }
      }
      return urls;
   }

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
   
   public Collection<URL> lookupFileLocations(String filename, ClassLoader cl) throws IOException{
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
   
}
