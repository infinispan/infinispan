/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.ComponentsJmxRegistration;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.util.logging.LogFactory;

public class LifecycleCallbacks extends AbstractModuleLifecycle {
   private static final Log log = LogFactory.getLog(LifecycleCallbacks.class, Log.class);

   private ObjectName interpreterObjName;

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
      String groupName = getGroupName(globalCfg);
      String jmxDomain = globalCfg.globalJmxStatistics().domain();
      Interpreter interpreter = new Interpreter();

      gcr.registerComponent(interpreter, Interpreter.class);

      // Pick up metadata from the component metadata repository
      ManageableComponentMetadata meta = gcr.getComponentMetadataRepo().findComponentMetadata(Interpreter.class)
            .toManageableComponentMetadata();
      // And use this metadata when registering the transport as a dynamic MBean
      try {
         ResourceDMBean mbean = new ResourceDMBean(interpreter, meta);
         interpreterObjName = new ObjectName(String.format("%s:%s,component=Interpreter", jmxDomain, groupName));
         JmxUtil.registerMBean(mbean, interpreterObjName, mbeanServer);
      } catch (Exception e) {
         interpreterObjName = null;
         log.jmxRegistrationFailed();
      }
   }

   private String getGroupName(GlobalConfiguration globalCfg) {
      return CacheManagerJmxRegistration.CACHE_MANAGER_JMX_GROUP + "," + ComponentsJmxRegistration.NAME_KEY + "="
            + ObjectName.quote(globalCfg.globalJmxStatistics().cacheManagerName());
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      if (interpreterObjName != null) {
         GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
         try {
            JmxUtil.unregisterMBean(interpreterObjName, mbeanServer);
         } catch (Exception e) {
            log.jmxUnregistrationFailed();
         }
      }
   }
}
