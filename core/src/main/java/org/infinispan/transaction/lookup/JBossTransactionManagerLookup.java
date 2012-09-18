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
package org.infinispan.transaction.lookup;

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.Util;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;


/**
 * Uses JNDI to look-up the {@link TransactionManager} instance from "java:/TransactionManager".
 *
 * @author Bela Ban, Aug 26 2003
 * @since 4.0
 */
public class JBossTransactionManagerLookup implements TransactionManagerLookup {

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      String as7Location = "java:jboss/TransactionManager";

      InitialContext initialContext = new InitialContext();
      try {
         // Check for JBoss AS 7
         return (TransactionManager) initialContext.lookup(as7Location);
      } catch (NamingException ne) {
         // Fall back and try for AS 4 ~ 6
         String legacyAsLocation = "java:/TransactionManager";

         try {
            // Check for JBoss AS 4 ~ 6
            return (TransactionManager) initialContext.lookup(legacyAsLocation);
         } catch (NamingException neAgain) {
            throw new ConfigurationException("Unable to locate a transaction manager in JNDI, either in " + as7Location + " or " + legacyAsLocation);
         }

      } finally {
         Util.close(initialContext);
      }
  }
}
