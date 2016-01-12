package org.infinispan.factories.components;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * This is a repository of component metadata, which is populated when the Infinispan core jar is loaded up.  Actual
 * metadata is analyzed and persisted into the jar at build-time, taking it off the critical path.
 *
 * @author Manik Surtani
 * @since 5.1
 * @see ComponentMetadata
 */
public class ComponentMetadataRepo {
   final Map<String, ComponentMetadata> componentMetadataMap = new HashMap<String, ComponentMetadata>(128);
   final Map<String, String> factories = new HashMap<String, String>(16);
   private final ComponentMetadata dependencyFreeComponent = new ComponentMetadata();

   @SuppressWarnings("unchecked")
   public synchronized void readMetadata(URL metadataFile) throws IOException, ClassNotFoundException {
      Map<String, ComponentMetadata> comp;
      Map<String, String> fact;
      InputStream inputStream = metadataFile.openStream();
      try {
         BufferedInputStream bis = new BufferedInputStream(inputStream);
         try {
            ObjectInputStream ois = new ObjectInputStream(bis);
            try {
               comp = (Map<String, ComponentMetadata>) ois.readObject();
               fact = (Map<String, String>) ois.readObject();
            } finally {
               ois.close();
            }
         } finally {
            bis.close();
         }
      } finally {
         inputStream.close();
      }

      componentMetadataMap.putAll(comp);
      factories.putAll(fact);
   }

   /**
    * Adds component metadata at runtime.
    * @param componentMetadata
    */
   public synchronized void addComponentMetadata(ComponentMetadata componentMetadata) {
      componentMetadataMap.put(componentMetadata.getName(), componentMetadata);
   }

   /**
    * Remove component metadata at runtime.
    * @param name of component type to remove
    */
   public synchronized ComponentMetadata removeComponentMetadata(String name) {
      return componentMetadataMap.remove(name);
   }

   /**
    * Locates metadata for a given component type if registered.  If not registered, superclasses/interfaces are
    * consulted, until, finally, an empty instance of {@link ComponentMetadata} is returned effectively declaring
    * that the component has no dependencies or any lifecycle methods declared.
    *
    * @param componentType component type to look for
    * @return metadata expressed as a ComponentMetadata instance
    */
   public ComponentMetadata findComponentMetadata(Class<?> componentType) {
      ComponentMetadata md = null;
      while (md == null) {
         String componentName = componentType.getName();
         md = componentMetadataMap.get(componentName);
         if (md == null) {
            // Test superclasses/superinterfaces.
            if (!componentType.equals(Object.class) && !componentType.isInterface())
               componentType = componentType.getSuperclass();
            else
               md = dependencyFreeComponent;
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
   public ComponentMetadata findComponentMetadata(String componentName) {
      return componentMetadataMap.get(componentName);
   }

   /**
    * Locates the fully qualified class name of a factory capable of constructing an instance of <pre>componentType</pre>.
    * Typically this is a factory annotated with {@link DefaultFactoryFor}.
    * @param componentType component to create
    * @return a factory, or null if not found.
    */
   public String findFactoryForComponent(Class<?> componentType) {
      return factories.get(componentType.getName());
   }

   /**
    * Initializes this repository.  The Iterable passed in should contain all {@link ModuleMetadataFileFinder} instances
    * for all loaded Infinispan modules.  Note that the core module is always loaded and need not be contained in this
    * iterable.
    * @param moduleMetadataFiles file finders to iterate through and load into the repository
    */
   public void initialize(Iterable<ModuleMetadataFileFinder> moduleMetadataFiles, ClassLoader cl) {
      // First init core module metadata
      try {
         readMetadata(FileLookupFactory.newInstance().lookupFileLocation("infinispan-core-component-metadata.dat", cl));
      } catch (Exception e) {
         throw new CacheException("Unable to load component metadata!", e);
      }

      // Now the modules
      for (ModuleMetadataFileFinder finder: moduleMetadataFiles) {
         try {
            readMetadata(FileLookupFactory.newInstance().lookupFileLocation(finder.getMetadataFilename(), cl));
         } catch (Exception e) {
            throw new CacheException("Unable to load component metadata in file " + finder.getMetadataFilename(), e);
         }
      }
   }

   /**
    * Inject a factory for a given component type.
    *
    * @param componentType Component type that the factory will produce
    * @param factoryType Factory that produces the given type of components
    */
   public void injectFactoryForComponent(Class<?> componentType, Class<?> factoryType) {
      factories.put(componentType.getName(), factoryType.getName());
   }

}
