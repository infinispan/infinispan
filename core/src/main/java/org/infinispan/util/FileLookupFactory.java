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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;


public class FileLookupFactory {

   public static FileLookup newInstance() {
      ClassLoader cl = FileLookup.class.getClassLoader();
      if (cl.getClass().getName().equals("org.osgi.framework.BundleReference"))
         return new OsgiFileLookup();
      else
         return new DefaultFileLookup();
      
   }

   
   public static class DefaultFileLookup extends AbstractFileLookup implements FileLookup {

      protected DefaultFileLookup() {
      }
      
      protected InputStream getAsInputStreamFromClassLoader(String filename, ClassLoader appClassLoader) {
         for (ClassLoader cl : Util.getClassLoaders(appClassLoader))  {
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
         for (ClassLoader cl : Util.getClassLoaders(userClassLoader))  {
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
         Collection<URL> urls = new HashSet<URL>();
         for (ClassLoader cl : Util.getClassLoaders(userClassLoader))  {
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
      
   }

   
}
