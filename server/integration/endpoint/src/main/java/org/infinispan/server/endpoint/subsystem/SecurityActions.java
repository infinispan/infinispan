/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010-2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.server.endpoint.subsystem;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.Security;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;

/**
 * Privileged Actions
 *
 * @author Anil.Saldhana@redhat.com
 * @since Jan 12, 2011
 */
class SecurityActions {
   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      if (System.getSecurityManager() != null) {
          return AccessController.doPrivileged(action);
      } else {
          return Security.doPrivileged(action);
      }
  }

   static void startProtocolServer(final ProtocolServer server, final ProtocolServerConfiguration configuration, final EmbeddedCacheManager cacheManager) {
      PrivilegedAction<Void> action = new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            server.start(configuration, cacheManager);
            return null;
         }
      };
      doPrivileged(action);
   }

}
