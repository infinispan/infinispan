/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.io.IOException;
import java.security.Principal;

import javax.security.jacc.PolicyContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.Wrapper;
import org.apache.catalina.authenticator.Constants;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.jboss.as.web.security.JBossGenericPrincipal;
import org.jboss.security.RunAsIdentity;
import org.jboss.security.SecurityConstants;
import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityRolesAssociation;
import org.jboss.security.SecurityUtil;

/**
 * A {@code Valve} that creates a {@code SecurityContext} if one doesn't exist and sets the security
 * information based on the authenticated principal in the request's session.
 *
 * @author <a href="mailto:mmoyses@redhat.com">Marcus Moyses</a>
 * @author Scott.Stark@jboss.org
 * @author Thomas.Diesler@jboss.org
 * @author Anil.Saldhana@jboss.org
 * @author Tristan Tarrant
 */
public class RestSecurityContext extends ValveBase {
   private static final ThreadLocal<Request> activeRequest = new ThreadLocal<Request>();
   private String contextSecurityDomain;
   private String name;

   public RestSecurityContext(String name, String contextSecurityDomain) {
      this.name = name;
      this.contextSecurityDomain = contextSecurityDomain;
   }

   /** {@inheritDoc} */
   @Override
   public void invoke(Request request, Response response) throws IOException, ServletException {
      activeRequest.set(request);

      Session session = null;
      // Get the request caller which could be set due to SSO
      Principal caller = request.getPrincipal();
      // The cached web container principal
      JBossGenericPrincipal principal = null;
      HttpSession hsession = request.getSession(false);

      boolean createdSecurityContext = false;
      SecurityContext sc = SecurityActions.getSecurityContext();
      if (sc == null) {
         createdSecurityContext = true;
         String securityDomain = SecurityUtil.unprefixSecurityDomain(contextSecurityDomain);
         if (securityDomain == null)
            securityDomain = SecurityConstants.DEFAULT_WEB_APPLICATION_POLICY;
         sc = SecurityActions.createSecurityContext(securityDomain);
         SecurityActions.setSecurityContextOnAssociation(sc);
      }

      try {
         Wrapper servlet = null;
         try {
            servlet = request.getWrapper();
            if (servlet != null) {
               String name = servlet.getName();
               RunAsIdentity runAsIdentity = null;
               SecurityActions.pushRunAsIdentity(runAsIdentity);
            }

            // If there is a session, get the tomcat session for the principal
            Manager manager = container.getManager();
            if (manager != null && hsession != null) {
               try {
                  session = manager.findSession(hsession.getId());
               } catch (IOException ignore) {
               }
            }

            if (caller == null || !(caller instanceof JBossGenericPrincipal)) {
               // Look to the session for the active caller security context
               if (session != null) {
                  principal = (JBossGenericPrincipal) session.getPrincipal();
               }
               if (principal == null) {
                  Session sessionInternal = request.getSessionInternal(false);
                  if (sessionInternal != null) {
                     principal = (JBossGenericPrincipal) sessionInternal.getNote(Constants.FORM_PRINCIPAL_NOTE);
                  }
               }
            } else {
               // Use the request principal as the caller identity
               principal = (JBossGenericPrincipal) caller;
            }

            // If there is a caller use this as the identity to propagate
            if (principal != null) {
               if (createdSecurityContext) {
                  sc.getUtil().createSubjectInfo(principal.getUserPrincipal(), principal.getCredentials(),
                        principal.getSubject());
               }
            }
         } catch (Throwable e) {
            ROOT_LOGGER.failedToDetermineServlet(e);
         }
         // set JACC contextID
         PolicyContext.setContextID(name);

         // Perform the request
         getNext().invoke(request, response);
         if (servlet != null) {
            SecurityActions.popRunAsIdentity();
         }
      } finally {
         SecurityActions.clearSecurityContext();
         SecurityRolesAssociation.setSecurityRoles(null);
         PolicyContext.setContextID(null);
         activeRequest.set(null);
      }
   }

   public static Request getActiveRequest() {
      return activeRequest.get();
   }

}
