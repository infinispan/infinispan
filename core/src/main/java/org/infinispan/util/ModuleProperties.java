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

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.config.ConfigurationException;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The <code>ModuleProperties</code> class represents Infinispan's module configuration key value
 * pairs. Each Infinispan module is required to provide accompanying infinispan-module.properties in
 * module's jar. An instance of this class represents in-memory representation of
 * infinispan-module.properties file.
 * <p>
 *
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ModuleProperties extends Properties {

    private static final long serialVersionUID = 2558131508076199744L;

    private static final Log log = LogFactory.getLog(ModuleProperties.class);
    public static final String MODULE_PROPERTIES_FILENAME = "infinispan-module.properties";
    public static final String MODULE_NAME_KEY = "infinispan.module.name";
    public static final String MODULE_CONFIGURATION_CLASS = "infinispan.module.configurationClassName";
    public static final String MODULE_LIFECYCLE = "infinispan.module.lifecycle";
    public static final String MODULE_COMMAND_INITIALIZER = "infinispan.module.command.initializer";
    public static final String MODULE_COMMAND_FACTORY = "infinispan.module.command.factory";
    private static Map<String,ModuleProperties> moduleProperties;
    private static Map<Byte,ModuleCommandFactory> commandFactories;
    private static Map<Byte,Class<? extends ModuleCommandInitializer>> commandInitializers;
   private static Collection<Class<? extends ReplicableCommand>> moduleCommands;

   protected static Enumeration<URL> getResources(String filename) throws IOException {
        Enumeration<URL> result;
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        result = cl == null ? null : cl.getResources(filename);
        if (result == null) {
            // check system class
            result = ModuleProperties.class.getClassLoader().getResources(filename);
        }
        return result;
    }

    public static ModuleProperties loadModuleProperties(String moduleName) throws IOException {

        Enumeration<URL> resources = getResources(MODULE_PROPERTIES_FILENAME);
        if (resources == null)
            throw new IOException("Could not find " + MODULE_PROPERTIES_FILENAME
                            + " files on classpath for module " + moduleName);

        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            ModuleProperties props = new ModuleProperties();
            InputStream inStream = url.openStream();
            try {
               props.load(inStream);
            } finally {
               Util.close(inStream);
            }
            props.verify();

            if (props.getName().equalsIgnoreCase(moduleName)) {
                return props;
            }
        }
        return null;
    }

   private static Map<String, ModuleProperties> loadModuleProperties() throws IOException {
      Map<String, ModuleProperties> map = new HashMap<String, ModuleProperties>();
      Enumeration<URL> resources = getResources(MODULE_PROPERTIES_FILENAME);
      if (resources != null) {
         while (resources.hasMoreElements()) {
            URL url = null;
            try {
               url = resources.nextElement();
               ModuleProperties props = new ModuleProperties();
               InputStream inStream = url.openStream();
               try {
                  props.load(inStream);
               } finally {
                  Util.close(inStream);
               }
               props.verify();
               map.put(props.getName(), props);
            } catch (Exception e) {
               log.couldNotLoadModuleAtUrl(url, e);
            }
         }
      }
      return map;
   }

   private static Map<String, ModuleProperties> getModuleProperties() throws IOException {
      if (moduleProperties == null) moduleProperties = loadModuleProperties();
      return moduleProperties;
   }

   public static List<ModuleLifecycle> resolveModuleLifecycles() {
      try {
         List<ModuleLifecycle> lifecycles = new ArrayList<ModuleLifecycle>();
         Map<String, ModuleProperties> p = ModuleProperties.getModuleProperties();
         for (Map.Entry<String, ModuleProperties> m : p.entrySet()) {
            try {
               String lifecycleClassName = m.getValue().getLifecycleClassName();
               if (lifecycleClassName != null && !lifecycleClassName.isEmpty()) {
               Class<?> loadClass = Util.loadClassStrict(lifecycleClassName, Thread.currentThread().getContextClassLoader());
                  Object proxy = Proxies.newCatchThrowableProxy(loadClass.newInstance());
                  ModuleLifecycle ml = (ModuleLifecycle) proxy;
                  lifecycles.add(ml);
               }

            } catch (Exception e) {
               log.couldNotInitializeModule(m.getKey(), e);
            }
         }
         return lifecycles;
      } catch (Exception e) {
         return Collections.emptyList();
      }
   }

    public String getName() {
        return super.getProperty(MODULE_NAME_KEY);
    }

    public String getConfigurationClassName() {
        return super.getProperty(MODULE_CONFIGURATION_CLASS);
    }

    public String getLifecycleClassName() {
        return super.getProperty(MODULE_LIFECYCLE);
    }

   public String getCommandInitializerClassName() {
      return super.getProperty(MODULE_COMMAND_INITIALIZER);
   }

   public String getCommandFactoryClassName() {
      return super.getProperty(MODULE_COMMAND_FACTORY);
   }

    protected void verify() {
        if (getName() == null)
            throw new ConfigurationException(
                            "Module properties does not specify module name. Module name should be specified using key "
                                            + MODULE_NAME_KEY);

       // we should not *require* that every module supplies these...

//        if (getConfigurationClassName() == null)
//            throw new ConfigurationException(
//                            "Module properties does not specify module configuration class name. Module configuration class name should be specified using key "
//                                            + MODULE_CONFIGURATION_CLASS);
//
//        if (getLifecycleClassName() == null)
//            throw new ConfigurationException(
//                            "Module properties does not specify module lifecycle class name. Module lifecycle class name should be specified using key "
//                                            + MODULE_LIFECYCLE);
    }

   @SuppressWarnings("unchecked")
   private static void loadModuleCommandHandlers() {
      try {
         // initialize these collections to be really small, memory efficient
         commandFactories = new HashMap<Byte, ModuleCommandFactory>(1);
         commandInitializers = new HashMap<Byte, Class<? extends ModuleCommandInitializer>>(1);
         moduleCommands = new HashSet<Class<? extends ReplicableCommand>>(1);

         Map<String, ModuleProperties> p = ModuleProperties.getModuleProperties();
         for (Map.Entry<String, ModuleProperties> module: p.entrySet()) {
            String factClass = module.getValue().getCommandFactoryClassName();
            String initClass = module.getValue().getCommandInitializerClassName();
            if (factClass != null && initClass != null) {
               try {
                  ModuleCommandFactory fact = (ModuleCommandFactory) Util.getInstance(factClass, Thread.currentThread().getContextClassLoader());
                  Class<? extends ModuleCommandInitializer> initClazz = Util.loadClass(initClass, Thread.currentThread().getContextClassLoader());
                  for (Map.Entry<Byte, Class<? extends ReplicableCommand>> entry: fact.getModuleCommands().entrySet()) {
                     byte id = entry.getKey();
                     if (commandFactories.containsKey(id))
                        throw new IllegalArgumentException("Module " + module.getKey() + " cannot use id " + id + " for commands, as it is already in use by " + commandFactories.get(id).getClass().getName());
                     commandFactories.put(id, fact);
                     commandInitializers.put(id, initClazz);
                     moduleCommands.add(entry.getValue());
                  }
               } catch (Exception e) {
                  throw new CacheException("Unable to load factory class " + factClass + " for module " + module.getKey());
               }
            }
         }
      } catch (IOException ioe) {
         commandInitializers = Collections.emptyMap();
         commandFactories = Collections.emptyMap();
         throw new CacheException("IO Exception reading module properties file!", ioe);
      }
   }

   public static Collection<Class<? extends ReplicableCommand>> moduleCommands() {
      if (moduleCommands == null) loadModuleCommandHandlers();
      return moduleCommands;
   }

   public static Map<Byte, ModuleCommandFactory> moduleCommandFactories() {
      if (commandFactories == null) loadModuleCommandHandlers();
      return commandFactories;
   }

   public static Map<Byte, ModuleCommandInitializer> moduleCommandInitializers() {
      if (commandInitializers == null) loadModuleCommandHandlers();
      if (commandInitializers.isEmpty())
         return Collections.emptyMap();
      else {
         Map<Byte, ModuleCommandInitializer> initializers = new HashMap<Byte, ModuleCommandInitializer>(commandInitializers.size());
         for (Map.Entry<Byte, Class<? extends ModuleCommandInitializer>> e: commandInitializers.entrySet())
            initializers.put(e.getKey(), Util.getInstance(e.getValue()));
         return initializers;
      }
   }
}
