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
package org.infinispan.transaction.tm;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAResource;

/**
 * Simple transaction manager implementation that maintains transaction state in memory only.
 *
 * @author bela
 *         <p/>
 *         Date: May 15, 2003 Time: 4:11:37 PM
 * @since 4.0
 */
public class DummyTransactionManager extends DummyBaseTransactionManager {

   protected static final Log log = LogFactory.getLog(DummyTransactionManager.class);

   private static final long serialVersionUID = 4396695354693176535L;

   public static DummyTransactionManager getInstance() {
      return LazyInitializeHolder.dummyTMInstance;
   }

   public static DummyUserTransaction getUserTransaction() {
      return LazyInitializeHolder.utx;
   }

   public static void destroy() {
      getInstance().setTransaction(null);
   }

   public XAResource firstEnlistedResource() {
      return getTransaction().firstEnlistedResource();
   }

   private static class LazyInitializeHolder {
      static final DummyTransactionManager dummyTMInstance = new DummyTransactionManager();
      static final DummyUserTransaction utx = new DummyUserTransaction(dummyTMInstance);
   }
}
