/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import org.apache.catalina.startup.ContextConfig;
import org.jboss.as.web.WebLogger;

/**
 *
 * RestContextConfig.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public class RestContextConfig extends ContextConfig {

   protected static org.jboss.logging.Logger log = org.jboss.logging.Logger.getLogger(RestContextConfig.class);

   @Override
   protected void completeConfig() {
      if (ok) {
         resolveServletSecurity();
      }

      if (ok) {
         validateSecurityRoles();
      }

      // Configure an authenticator if we need one
      if (ok) {
         authenticatorConfig();
      }

      // Make our application unavailable if problems were encountered
      if (!ok) {
         WebLogger.WEB_LOGGER.unavailable(context.getName());
         context.setConfigured(false);
      }
   }

}
