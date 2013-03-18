/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.remoting.transport;

import org.infinispan.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;

import java.util.Map;

/**
 * Common transport-related behaviour
 *
 * @author Manik Surtani
 * @version 4.2
 */
public abstract class AbstractTransport implements Transport {

   protected GlobalConfiguration configuration;

   @Inject
   @SuppressWarnings("unused")
   public void setConfiguration(GlobalConfiguration globalConfiguration) {
      this.configuration = globalConfiguration;
   }

   private boolean shouldThrowException(Exception ce) {
      if (!configuration.transport().strictPeerToPeer()) {
         if (ce instanceof NamedCacheNotFoundException) return false;
         if (ce.getCause() != null && ce.getCause() instanceof NamedCacheNotFoundException) return false;
      }
      return true;
   }

   public final boolean checkResponse(Object responseObject, Address sender) throws Exception {
      Log log = getLog();
      if (responseObject instanceof Response) {
         Response response = (Response) responseObject;
         if (response instanceof ExceptionResponse) {
            ExceptionResponse exceptionResponse = (ExceptionResponse) response;
            Exception e = exceptionResponse.getException();
            // if we have any application-level exceptions make sure we throw them!!
            if (shouldThrowException(e)) {
               throw log.remoteException(sender, e);
            } else {
               if (log.isDebugEnabled())
                  log.debug("Received exception from " + sender, e);
            }
         }
         return true;
      } else if (responseObject != null) {
         // null responses should just be ignored, all other responses should trigger an exception
         Class<?> responseClass = responseObject.getClass();
         log.tracef("Unexpected response object type from %s: %s", sender, responseClass);
         throw new CacheException(String.format("Unexpected response object type from %s: %s", sender, responseClass));
      }
      return false;
   } 
   
   protected final boolean parseResponseAndAddToResponseList(Object responseObject, Throwable exception, Map<Address, Response> responseListToAddTo, boolean wasSuspected,
                                                       boolean wasReceived, Address sender, boolean usedResponseFilter, boolean ignoreLeavers)
           throws Exception
   {
      Log log = getLog();
      boolean invalidResponse = true;
      if (!wasSuspected && wasReceived) {
         invalidResponse = false;
         if (exception != null) {
            log.tracef(exception, "Unexpected exception from %s", sender);
            throw new CacheException("Remote (" + sender + ") failed unexpectedly", exception);
         }
         
         if (checkResponse(responseObject, sender)) responseListToAddTo.put(sender, (Response) responseObject);
      } else if (wasSuspected) {
         if (!ignoreLeavers) {
            throw new SuspectException("Suspected member: " + sender, sender);
         } else {
            log.tracef("Target node %s left during remote call, ignoring", sender);
            // Don't throw a TimeoutException in invokeRemotely if the only target left the cluster
            invalidResponse = false;
         }
      } else {
         // if we have a response filter then we may not have waited for some nodes!
         if (!usedResponseFilter) throw new TimeoutException("Replication timeout for " + sender);
      }

      return invalidResponse;
   }
}
