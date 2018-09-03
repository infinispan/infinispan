package org.infinispan.factories.components;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.jcip.annotations.GuardedBy;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This is a repository of component metadata, which is populated when the Infinispan core jar is loaded up.  Actual
 * metadata is analyzed and persisted into the jar at build-time, taking it off the critical path.
 *
 * @author Manik Surtani
 * @since 5.1
 * @see ComponentMetadata
 */
public class ComponentMetadataRepo {
   private static final Log log = LogFactory.getLog(ComponentMetadataRepo.class);
   final Map<String, ComponentMetadata> componentMetadataMap = new HashMap<>(128);
   final Map<String, String> factories = new HashMap<>(16);
   private final ComponentMetadata dependencyFreeComponent = new ComponentMetadata();

   @SuppressWarnings("unchecked")
   public synchronized void readMetadata(URL metadataFile) throws IOException, ClassNotFoundException {
      Map<String, ComponentMetadata> comp;
      Map<String, String> fact;
      try (InputStream inputStream = metadataFile.openStream()) {
         try (BufferedInputStream bis = new BufferedInputStream(inputStream)) {
            try (ObjectInputStream ois = new ObjectInputStream(bis)){
               comp = (Map<String, ComponentMetadata>) ois.readObject();
               fact = (Map<String, String>) ois.readObject();
            }
         }
      }

      componentMetadataMap.putAll(comp);
      factories.putAll(fact);
      if (log.isTraceEnabled()) {
         log.tracef("Loaded metadata from '%s': %d components, %d factories", metadataFile, comp.size(), fact.size());
      }
   }

   /**
    * Locates metadata for a given component type if registered.  If not registered, superclasses/interfaces are
    * consulted, until, finally, an empty instance of {@link ComponentMetadata} is returned effectively declaring
    * that the component has no dependencies or any lifecycle methods declared.
    *
    * @param componentType component type to look for
    * @return metadata expressed as a ComponentMetadata instance
    * @deprecated Since 9.4, please use {@link #findComponentMetadata(String)} instead.
    */
   @Deprecated
   public ComponentMetadata findComponentMetadata(Class<?> componentType) {
      ComponentMetadata md = null;
      while (md == null) {
         String componentName = componentType.getName();
         md = findComponentMetadata(componentName);
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
    * @deprecated Since 9.4, please use {@link #findComponentMetadata(String)} instead.
    */
   @Deprecated
   public ComponentMetadata findComponentMetadata(String componentName) {
      return componentMetadataMap.get(componentName);

   }

   /**
    * Look up metadata for a component class.
    *
    * <p>If the class does not have any metadata, tries to look up the metadata of its superclasses.
    * This is needed for mocks and other classes generated at runtime.</p>
    *
    * <p>Do not use for looking up the metadata of an interface,
    * e.g. to determine the scope of a component that doesn't exist.</p>
    */
   public ComponentMetadata getComponentMetadata(Class<?> componentClass) {
      ComponentMetadata md = componentMetadataMap.get(componentClass.getName());
      if (md == null) {
         if (componentClass.getSuperclass() != null) {
            return getComponentMetadata(componentClass.getSuperclass());
         } else {
            return null;
         }
      }

      initMetadata(componentClass, md);

      return md;
   }

   private void initMetadata(Class<?> componentClass, ComponentMetadata md) {
      if (md.clazz == null) {
         synchronized (this) {
            if (md.clazz == null) {
               initInjectionFields(md, componentClass, componentClass.getClassLoader());
               initInjectionMethods(md, componentClass, componentClass.getClassLoader());
               initLifecycleMethods(md.getStartMethods(), componentClass);
               initLifecycleMethods(md.getPostStartMethods(), componentClass);
               initLifecycleMethods(md.getStopMethods(), componentClass);
               md.clazz = componentClass;
            }
         }
      }

      if (md.clazz != componentClass) {
         throw new IllegalStateException("Component metadata has the wrong type: " + md.clazz + ", expected: " + componentClass);
      }
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
    * Locates the fully qualified class name of a factory capable of constructing an instance of <pre>componentType</pre>.
    * Typically this is a factory annotated with {@link DefaultFactoryFor}.
    * @param componentName component to create
    * @return a factory, or null if not found.
    */
   public String findFactoryForComponent(String componentName) {
      return factories.get(componentName);
   }

   /**
    * Initializes this repository.  The Iterable passed in should contain all {@link ModuleMetadataFileFinder} instances
    * for all loaded Infinispan modules.  Note that the core module is always loaded and need not be contained in this
    * iterable.
    * @param moduleMetadataFiles file finders to iterate through and load into the repository
    */
   public void initialize(Iterable<ModuleMetadataFileFinder> moduleMetadataFiles, ClassLoader cl) {
      // First init core module metadata
      FileLookup fileLookup = FileLookupFactory.newInstance();
      try {
         readMetadata(fileLookup.lookupFileLocation("infinispan-core-component-metadata.dat", cl));
      } catch (Exception e) {
         throw new CacheException("Unable to load component metadata!", e);
      }

      // Now the modules
      for (ModuleMetadataFileFinder finder: moduleMetadataFiles) {
         try {
            readMetadata(fileLookup.lookupFileLocation(finder.getMetadataFilename(), cl));
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
    * @deprecated For testing purposes only.
    */
   @Deprecated
   public void injectFactoryForComponent(Class<?> componentType, Class<?> factoryType) {
      factories.put(componentType.getName(), factoryType.getName());
   }

   public boolean hasFactory(String name) {
      return factories.containsKey(name);
   }

   @GuardedBy("this")
   private void initInjectionMethods(ComponentMetadata metadata, Class<?> componentClass, ClassLoader classLoader) {
      ComponentMetadata.InjectMethodMetadata[] injectionMethods = metadata.getInjectMethods();
      if (injectionMethods != null && injectionMethods.length > 0) {
         for (ComponentMetadata.InjectMethodMetadata methodMetadata : injectionMethods) {
            try {
               String[] parameters = methodMetadata.getParameters();
               Class<?>[] parameterClasses = new Class[parameters.length];
               for (int i = 0; i < parameters.length; i++) {
                  String parameter = parameters[i];
                  parameterClasses[i] = methodMetadata.getParameterLazy(i) ? ComponentRef.class :
                                        ReflectionUtil.getClassForName(parameter, classLoader);
               }
               methodMetadata.setParameterClasses(parameterClasses);

               Method m = ReflectionUtil.findMethod(componentClass, methodMetadata.getMethodName(), parameterClasses);
               methodMetadata.setMethod(m);
            } catch (ClassNotFoundException e) {
               throw new CacheConfigurationException(e);
            }
         }
      }
   }

   @GuardedBy("this")
   private void initInjectionFields(ComponentMetadata metadata, Class<?> componentClass,
                                    ClassLoader classLoader) {
      ComponentMetadata.InjectFieldMetadata[] injectionFields = metadata.getInjectFields();
      if (injectionFields != null && injectionFields.length > 0) {
         for (ComponentMetadata.InjectFieldMetadata fieldMetadata : injectionFields) {
            Class<?> declarationClass = componentClass;
            while (!declarationClass.getName().equals(fieldMetadata.getFieldClassName())) {
               declarationClass = declarationClass.getSuperclass();
            }
            Field f = ReflectionUtil.getField(fieldMetadata.getFieldName(), declarationClass);
            fieldMetadata.setField(f);
            fieldMetadata.setComponentClass(Util.loadClass(fieldMetadata.getComponentType(), classLoader));
         }
      }
   }

   @GuardedBy("this")
   private void initLifecycleMethods(ComponentMetadata.PrioritizedMethodMetadata[] prioritizedMethods,
         Class<?> componentClass) {
      for (ComponentMetadata.PrioritizedMethodMetadata prioritizedMethod : prioritizedMethods) {
         Method method = ReflectionUtil.findMethod(componentClass, prioritizedMethod.getMethodName());
         prioritizedMethod.setMethod(method);
      }
      if (prioritizedMethods.length > 1) {
         Arrays.sort(prioritizedMethods, (a, b) -> a.getPriority() - b.getPriority());
      }
   }
}
