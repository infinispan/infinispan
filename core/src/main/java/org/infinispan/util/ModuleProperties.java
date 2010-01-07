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
package org.infinispan.util;

import org.infinispan.config.ConfigurationException;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
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
            props.load(url.openStream());
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
               props.load(url.openStream());
               props.verify();
               map.put(props.getName(), props);
            } catch (Exception e) {
               log.warn("Could not load module at URL " + url, e);
            }
         }
      }
      return map;
   }
    
   public static List<ModuleLifecycle> resolveModuleLifecycles() throws Exception {
      List<ModuleLifecycle> lifecycles = new ArrayList<ModuleLifecycle>();
      Map<String, ModuleProperties> p = ModuleProperties.loadModuleProperties();
      for (Map.Entry<String, ModuleProperties> m : p.entrySet()) {
         try {
            String lifecycleClassName = m.getValue().getLifecycleClassName();
            Class<?> loadClass = Util.loadClass(lifecycleClassName);
            ModuleLifecycle ml = (ModuleLifecycle) Proxies.newCatchThrowableProxy((ModuleLifecycle) loadClass.newInstance());
            lifecycles.add(ml);
         } catch (Exception e) {
            log.warn("Module " + m.getKey() + " loaded, but could not be initialized ", e);
         }
      }
      return lifecycles;
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

    protected void verify() {
        if (getName() == null)
            throw new ConfigurationException(
                            "Module propertes does not specify module name. Module name should be specified using key "
                                            + MODULE_NAME_KEY);
        if (getConfigurationClassName() == null)
            throw new ConfigurationException(
                            "Module propertes does not specify module configuration class name. Module configuration class name should be specified using key "
                                            + MODULE_CONFIGURATION_CLASS);
        
        if (getLifecycleClassName() == null)
            throw new ConfigurationException(
                            "Module propertes does not specify module lifecycle class name. Module lifecycle class name should be specified using key "
                                            + MODULE_LIFECYCLE);
    }
}
