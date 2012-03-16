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
package org.infinispan.commands.control;

import org.infinispan.CacheException;
import org.infinispan.cacheviews.CacheView;
import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * A control command to coordinate the installation of cache views between the members of the cluster.
 * The coordinator will receive REQUEST_JOIN and REQUEST_LEAVE commands from the nodes starting or stopping a cache.
 * It then sends PREPARE_VIEW to all the nodes in the cluster.
 * If all nodes return a successful response, the coordinator then sends a COMMIT_VIEW command to everyone.
 * If there is a failure on one of the nodes, the coordinator cancels the view with a ROLLBACK_VIEW command.
 *
 * @author Dan Berindei <dan@infinispan.org>
 * @since 5.1
 */
public class CacheViewControlCommand implements CacheRpcCommand {

   public enum Type {
      // a node is requesting to join the cluster
      REQUEST_JOIN,
      // a member is signaling that it wants to leave the cluster
      REQUEST_LEAVE,
      // the coordinator is signaling the other nodes to install a new view
      PREPARE_VIEW,
      // the coordinator is signalling the other nodes that everyone has finished installing the new view
      COMMIT_VIEW,
      // the coordinator is signalling that the current view has been cancelled and we are going back to the previous view
      ROLLBACK_VIEW,
      // when the coordinator changes (e.g. after a merge), the new coordinator requests existing state from all the members
      RECOVER_VIEW
   }

   private static final Log log = LogFactory.getLog(CacheViewControlCommand.class);

   public static final int COMMAND_ID = 17;

   private CacheViewsManager cacheViewsManager;

   private final String cacheName;
   private Type type;
   private Address sender;
   private int newViewId;
   private List<Address> newMembers;
   private int oldViewId;
   private List<Address> oldMembers;

   // For CommandIdUniquenessTest only
   public CacheViewControlCommand() {
      this.cacheName = null;
   }

   public CacheViewControlCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   public CacheViewControlCommand(String cacheName, Type type, Address sender, int newViewId, List<Address> newMembers, int oldViewId, List<Address> oldMembers) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.newViewId = newViewId;
      this.newMembers = newMembers;
      this.oldViewId = oldViewId;
      this.oldMembers = oldMembers;
   }

   public CacheViewControlCommand(String cacheName, Type type, Address sender, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.newViewId = viewId;
   }

   public CacheViewControlCommand(String cacheName, Type type, Address sender) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
   }

   public void init(CacheViewsManager cacheViewsManager) {
      this.cacheViewsManager = cacheViewsManager;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         switch (type) {
            case REQUEST_JOIN:
               cacheViewsManager.handleRequestJoin(sender, cacheName);
               return null;
            case REQUEST_LEAVE:
               cacheViewsManager.handleRequestLeave(sender, cacheName);
              return null;
            case PREPARE_VIEW:
               cacheViewsManager.handlePrepareView(cacheName, new CacheView(newViewId, newMembers),
                     new CacheView(oldViewId, oldMembers));
               return null;
            case COMMIT_VIEW:
               cacheViewsManager.handleCommitView(cacheName, newViewId);
               return null;
            case ROLLBACK_VIEW:
               cacheViewsManager.handleRollbackView(cacheName, newViewId, oldViewId);
               return null;
            case RECOVER_VIEW:
               return cacheViewsManager.handleRecoverViews();
            default:
               throw new CacheException("Unknown cache views control command type " + type);
         }
      } catch (Throwable t) {
         log.exceptionHandlingCommand(this, t);
         throw t;
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public void setOrigin(Address origin) {
      this.sender = origin;
   }

   @Override
   public Address getOrigin() {
      return sender;
   }

   public Type getType() {
      return type;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{(byte) type.ordinal(), sender, newViewId, newMembers, oldViewId, oldMembers};
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      newViewId = (Integer) parameters[i++];
      newMembers = (List<Address>) parameters[i++];
      oldViewId = (Integer) parameters[i++];
      oldMembers = (List<Address>) parameters[i++];
   }

   @Override
   public String toString() {
      return "CacheViewControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", newViewId=" + newViewId +
            ", newMembers=" + newMembers +
            ", oldViewId=" + oldViewId +
            ", oldMembers=" + oldMembers +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
