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

/**
 * Not really a transaction manager in the truest sense of the word.  Only used to batch up operations.  Proper
 * transactional semantics of rollbacks and recovery are NOT used here.
 *
 * @author bela
 * @since 4.0
 */
public class BatchModeTransactionManager extends DummyBaseTransactionManager {
	
   private static final long serialVersionUID = 5656602677430350961L;

   static BatchModeTransactionManager instance = null;

   public static BatchModeTransactionManager getInstance() {
      if (instance == null) {
         instance = new BatchModeTransactionManager();
      }
      return instance;
   }

   public static void destroy() {
      if (instance == null) return;
      instance.setTransaction(null);
      instance = null;
   }

}
