/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

package org.jboss.as.clustering.infinispan.subsystem;

import javax.security.auth.Subject;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.jboss.security.audit.AuditEvent;
import org.jboss.security.audit.AuditLevel;
import org.jboss.security.audit.AuditManager;

/**
 * ServerAuditLogger.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ServerAuditLogger implements AuditLogger {

   private final AuditManager auditManager;

   ServerAuditLogger(AuditManager auditManager) {
      this.auditManager = auditManager;
   }

   @Override
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission, AuditResponse response) {
      String level;
      switch (response) {
      case ALLOW:
         level = AuditLevel.SUCCESS;
         break;
      case DENY:
         level = AuditLevel.FAILURE;
         break;
      case ERROR:
         level = AuditLevel.ERROR;
         break;
      default:
         level = AuditLevel.INFO;
         break;
      }
      AuditEvent ae = new AuditEvent(level);
      auditManager.audit(ae);
   }

}
