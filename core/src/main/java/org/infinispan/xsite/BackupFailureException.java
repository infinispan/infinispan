/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.xsite;

import org.infinispan.remoting.RpcException;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception to be used to signal failures to backup to remote sites.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupFailureException extends RpcException {

   private Map<String,Throwable> failures;
   private String                localCacheName;


   public BackupFailureException(String localCacheName) {
      this.localCacheName  = localCacheName;
   }

   public BackupFailureException() {
   }

   public void addFailure(String site, Throwable t) {
      if(site != null && t != null) {
         if(failures == null)
            failures = new HashMap<String,Throwable>(3);
         failures.put(site, t);
      }
   }

   public String getRemoteSiteNames() {
      return failures != null? failures.keySet().toString() : null;
   }

   public String getLocalCacheName() {
      return localCacheName;
   }

   @Override
   public String toString() {
      if(failures == null || failures.isEmpty())
         return super.toString();
      StringBuilder sb=new StringBuilder("The local cache " + localCacheName + " failed to backup data to the remote sites:\n");
      for(Map.Entry<String,Throwable> entry: failures.entrySet())
         sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      return sb.toString();
   }
}
