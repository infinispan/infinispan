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
package org.horizon.transaction;

import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;

/**
 * A transaction manager lookup class that attempts to locate a TransactionManager. A variety of different classes and
 * JNDI locations are tried, for servers such as: <ul> <li> JBoss <li> JRun4 <li> Resin <li> Orion <li> JOnAS <li> BEA
 * Weblogic <li> Websphere 4.0, 5.0, 5.1, 6.0 <li> Sun, Glassfish </ul> If a transaction manager is not found, returns a
 * {@link org.horizon.transaction.DummyTransactionManager}.
 *
 * @author Markus Plesser
 * @since 4.0
 */
public class GenericTransactionManagerLookup implements TransactionManagerLookup {

   private static final Log log = LogFactory.getLog(GenericTransactionManagerLookup.class);

   /**
    * JNDI lookups performed?
    */
   private static boolean lookupDone = false;

   /**
    * No JNDI available?
    */
   private static boolean lookupFailed = false;

   /**
    * The JVM TransactionManager found.
    */
   private static TransactionManager tm = null;

   /**
    * JNDI locations for TransactionManagers we know of
    */
   private static String[][] knownJNDIManagers =
         {
               {"java:/TransactionManager", "JBoss, JRun4"},
               {"java:comp/TransactionManager", "Resin 3.x"},
               {"java:appserver/TransactionManager", "Sun Glassfish"},
               {"java:pm/TransactionManager", "Borland, Sun"},
               {"javax.transaction.TransactionManager", "BEA WebLogic"},
               {"java:comp/UserTransaction", "Resin, Orion, JOnAS (JOTM)"},
         };

   /**
    * WebSphere 5.1 and 6.0 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_5_1 = "com.ibm.ws.Transaction.TransactionManagerFactory";

   /**
    * WebSphere 5.0 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_5_0 = "com.ibm.ejs.jts.jta.TransactionManagerFactory";

   /**
    * WebSphere 4.0 TransactionManagerFactory
    */
   private static final String WS_FACTORY_CLASS_4 = "com.ibm.ejs.jts.jta.JTSXA";

   /**
    * Get the systemwide used TransactionManager
    *
    * @return TransactionManager
    */
   public TransactionManager getTransactionManager() {
      if (!lookupDone)
         doLookups();
      if (tm != null)
         return tm;
      if (lookupFailed) {
         //fall back to a dummy from Horizon
         tm = DummyTransactionManager.getInstance();
         log.warn("Falling back to DummyTransactionManager from Horizon");
      }
      return tm;
   }

   /**
    * Try to figure out which TransactionManager to use
    */
   private static void doLookups() {
      if (lookupFailed)
         return;
      InitialContext ctx;
      try {
         ctx = new InitialContext();
      }
      catch (NamingException e) {
         log.error("Failed creating initial JNDI context", e);
         lookupFailed = true;
         return;
      }
      //probe jndi lookups first
      for (String[] knownJNDIManager : knownJNDIManagers) {
         Object jndiObject;
         try {
            if (log.isDebugEnabled())
               log.debug("Trying to lookup TransactionManager for " + knownJNDIManager[1]);
            jndiObject = ctx.lookup(knownJNDIManager[0]);
         }
         catch (NamingException e) {
            log.debug("Failed to perform a lookup for [" + knownJNDIManager[0] + " (" + knownJNDIManager[1]
                  + ")]");
            continue;
         }
         if (jndiObject instanceof TransactionManager) {
            tm = (TransactionManager) jndiObject;
            log.debug("Found TransactionManager for " + knownJNDIManager[1]);
            return;
         }
      }
      //try to find websphere lookups since we came here
      Class clazz;
      try {
         log.debug("Trying WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
         clazz = Class.forName(WS_FACTORY_CLASS_5_1);
         log.debug("Found WebSphere 5.1: " + WS_FACTORY_CLASS_5_1);
      }
      catch (ClassNotFoundException ex) {
         try {
            log.debug("Trying WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
            clazz = Class.forName(WS_FACTORY_CLASS_5_0);
            log.debug("Found WebSphere 5.0: " + WS_FACTORY_CLASS_5_0);
         }
         catch (ClassNotFoundException ex2) {
            try {
               log.debug("Trying WebSphere 4: " + WS_FACTORY_CLASS_4);
               clazz = Class.forName(WS_FACTORY_CLASS_4);
               log.debug("Found WebSphere 4: " + WS_FACTORY_CLASS_4);
            }
            catch (ClassNotFoundException ex3) {
               log.debug("Couldn't find any WebSphere TransactionManager factory class, neither for WebSphere version 5.1 nor 5.0 nor 4");
               lookupFailed = true;
               return;
            }
         }
      }
      try {
         Class[] signature = null;
         Object[] args = null;
         Method method = clazz.getMethod("getTransactionManager", signature);
         tm = (TransactionManager) method.invoke(null, args);
      }
      catch (Exception ex) {
         log.error("Found WebSphere TransactionManager factory class [" + clazz.getName()
               + "], but couldn't invoke its static 'getTransactionManager' method", ex);
      }
   }

}
