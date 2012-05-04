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

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import java.lang.reflect.Method;

/**
 * JTA standalone TM lookup.
 *
 * @author Jason T. Greene
 * @since 4.0
 */
public class JBossStandaloneJTAManagerLookup implements TransactionManagerLookup {
   private Method manager, user;
   private static final Log log = LogFactory.getLog(JBossStandaloneJTAManagerLookup.class);
   
   @Inject
   public void init(Configuration configuration) {
      // The TM may be deployed embedded alongside the app, so this needs to be looked up on the same CL as the Cache
      try {
         manager = Util.loadClass("com.arjuna.ats.jta.TransactionManager", configuration.classLoader()).getMethod("transactionManager");
         user = Util.loadClass("com.arjuna.ats.jta.UserTransaction", configuration.classLoader()).getMethod("userTransaction");
      } catch (SecurityException e) {
         throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      TransactionManager tm = (TransactionManager) manager.invoke(null);
      if (log.isInfoEnabled()) log.retrievingTm(tm);
      return tm;
   }

   public UserTransaction getUserTransaction() throws Exception {
      return (UserTransaction) user.invoke(null);
   }
}