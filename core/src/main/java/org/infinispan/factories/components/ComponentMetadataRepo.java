/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.factories.components;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a repository of component metadata, which is populated when the Infinispan core jar is loaded up.  Actual
 * metadata is analyzed and persisted into the jar at build-time, taking it off the critical path.
 *
 * @author Manik Surtani
 * @since 5.1
 * @see ComponentMetadata
 */
public class ComponentMetadataRepo {
   static final Map<String, ComponentMetadata> COMPONENT_METADATA_MAP = new HashMap<String, ComponentMetadata>(128);
   static final Map<String, String> FACTORIES = new HashMap<String, String>(16);
   private static final ComponentMetadata DEPENDENCY_FREE_COMPONENT = new ComponentMetadata();
   private static final Log log = LogFactory.getLog(ComponentMetadataRepo.class);

   @SuppressWarnings("unchecked")
   public synchronized static void readMetadata(URL metadataFile) throws IOException, ClassNotFoundException {
      BufferedInputStream bis = new BufferedInputStream(metadataFile.openStream());
      ObjectInputStream ois = new ObjectInputStream(bis);

      Map<String, ComponentMetadata> comp = (Map<String, ComponentMetadata>) ois.readObject();
      Map<String, String> fact = (Map<String, String>) ois.readObject();

      COMPONENT_METADATA_MAP.putAll(comp);
      FACTORIES.putAll(fact);
   }

   /**
    * Locates metadata for a given component type if registered.  If not registered, superclasses/interfaces are
    * consulted, until, finally, an empty instance of {@link ComponentMetadata} is returned effectively declaring
    * that the component has no dependencies or any lifecycle methods declared.
    *
    * @param componentType component type to look for
    * @return metadata expressed as a ComponentMetadata instance
    */
   public static ComponentMetadata findComponentMetadata(Class<?> componentType) {
      ComponentMetadata md = null;
      while (md == null) {
         String componentName = componentType.getName();
         md = COMPONENT_METADATA_MAP.get(componentName);
         if (md == null) {
            // Test superclasses/superinterfaces.
            if (!componentType.equals(Object.class) && !componentType.isInterface())
               componentType = componentType.getSuperclass();
            else
               md = DEPENDENCY_FREE_COMPONENT;
         }
      }

      return md;
   }

   /**
    * Locates metadata for a given component type if registered.  If not registered, a null is returned.
    *
    * @param componentName name of component type to look for
    * @return metadata expressed as a ComponentMetadata instance, or null
    */
   public static ComponentMetadata findComponentMetadata(String componentName) {
      return COMPONENT_METADATA_MAP.get(componentName);
   }

   /**
    * Locates the fully qualified class name of a factory capable of constructing an instance of <pre>componentType</pre>.
    * Typically this is a factory annotated with {@link DefaultFactoryFor}.
    * @param componentType component to create
    * @return a factory, or null if not found.
    */
   public static String findFactoryForComponent(Class<?> componentType) {
      return FACTORIES.get(componentType.getName());
   }

   /**
    * Initializes this repository.  The Iterable passed in should contain all {@link ModuleMetadataFileFinder} instances
    * for all loaded Infinispan modules.  Note that the core module is always loaded and need not be contained in this
    * iterable.
    * @param moduleMetadataFiles file finders to iterate through and load into the repository
    */
   public static void initialize(Iterable<ModuleMetadataFileFinder> moduleMetadataFiles, ClassLoader cl) {
      // First init core module metadata
      try {
         readMetadata(cl.getResource("infinispan-core-component-metadata.dat"));
      } catch (Exception e) {
         throw new CacheException("Unable to load component metadata!", e);
      }

      // Now the modules
      for (ModuleMetadataFileFinder finder: moduleMetadataFiles) {
         try {
            readMetadata(cl.getResource(finder.getMetadataFilename()));
         } catch (Exception e) {
            throw new CacheException("Unable to load component metadata in file " + finder.getMetadataFilename(), e);
         }
      }

   }
}
