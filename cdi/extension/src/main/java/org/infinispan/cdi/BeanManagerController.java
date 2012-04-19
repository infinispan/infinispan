/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.cdi;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.inject.spi.BeanManager;

import org.jboss.solder.beanManager.BeanManagerAware;

public class BeanManagerController extends BeanManagerAware {
   
   private Map<ClassLoader, BeanManager> beanManagers = new ConcurrentHashMap<ClassLoader, BeanManager>();

   public BeanManager getRegisteredBeanManager() {
      ClassLoader classLoader = getClassLoader(null);
      
      BeanManager bm = resolveBeanManager(classLoader);

      if (bm == null) {
         bm = getBeanManager();

         if (bm == null) {
            throw new IllegalStateException("Can not find BeanManager. Check CDI setup in your execution environment!");
         }

         registerBeanManager(classLoader, bm);

      }
      return bm;
   }
   
   public void registerBeanManager(ClassLoader cl, BeanManager bm){
      beanManagers.put(cl, bm);
   }
   
   public void registerBeanManager(BeanManager bm){
      beanManagers.put(getClassLoader(null), bm);
   }
   
   public void deregisterBeanManager(ClassLoader cl){
      beanManagers.remove(cl);
   }
   
   public void deregisterBeanManager(){
      beanManagers.remove(getClassLoader(null));
   }
   
   public BeanManager resolveBeanManager(ClassLoader cl){
      return beanManagers.get(cl);
   }

   public ClassLoader getClassLoader(Object o) {
      ClassLoader loader = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {

         @Override
         public ClassLoader run() {
            try {
               return Thread.currentThread().getContextClassLoader();
            } catch (Exception e) {
               return null;
            }
         }
      });

      if (loader == null && o != null) {
         loader = o.getClass().getClassLoader();
      }

      if (loader == null) {
         loader = BeanManagerController.class.getClassLoader();
      }

      return loader;
   }
}
