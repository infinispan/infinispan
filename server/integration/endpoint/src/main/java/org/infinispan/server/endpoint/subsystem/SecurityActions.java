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
import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityContextAssociation;

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
   /**
    * Set the {@code SecurityContext} on the {@code SecurityContextAssociation}
    *
    * @param sc
    *           the security context
    */
   static void setSecurityContextOnAssociation(final SecurityContext sc) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {

         @Override
         public Void run() {
            SecurityContextAssociation.setSecurityContext(sc);
            return null;
         }
      });
   }

   /**
    * Get the current {@code SecurityContext}
    *
    * @return an instance of {@code SecurityContext}
    */
   static SecurityContext getSecurityContext() {
      return AccessController.doPrivileged(new PrivilegedAction<SecurityContext>() {
         @Override
         public SecurityContext run() {
            return SecurityContextAssociation.getSecurityContext();
         }
      });
   }

   /**
    * Clears current {@code SecurityContext}
    */
   static void clearSecurityContext() {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            SecurityContextAssociation.clearSecurityContext();
            return null;
         }
      });
   }

   public static final String AUTH_EXCEPTION_KEY = "org.jboss.security.exception";

   static void clearAuthException() {
      if (System.getSecurityManager() != null) {
         AccessController.doPrivileged(new PrivilegedAction<Void>() {

            @Override
            public Void run() {
               SecurityContext sc = getSecurityContext();
               if (sc != null)
                  sc.getData().put(AUTH_EXCEPTION_KEY, null);
               return null;
            }
         });
      } else {
         SecurityContext sc = getSecurityContext();
         if (sc != null)
            sc.getData().put(AUTH_EXCEPTION_KEY, null);
      }
   }

   static Throwable getAuthException() {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged(new PrivilegedAction<Throwable>() {

            @Override
            public Throwable run() {
               SecurityContext sc = getSecurityContext();
               Throwable exception = null;
               if (sc != null)
                  exception = (Throwable) sc.getData().get(AUTH_EXCEPTION_KEY);
               return exception;
            }
         });
      } else {
         SecurityContext sc = getSecurityContext();
         Throwable exception = null;
         if (sc != null)
            exception = (Throwable) sc.getData().get(AUTH_EXCEPTION_KEY);
         return exception;
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
