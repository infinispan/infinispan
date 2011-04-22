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
package org.infinispan.tools.schema;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * TreeWalker abstract super class that should be extended for a particular tool
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public abstract class AbstractTreeWalker implements TreeWalker{

   protected final Log log;
   
   public AbstractTreeWalker() {
      super();
      log = LogFactory.getLog(getClass());
   }

   public void levelOrderTraverse(TreeNode root) {          
      Queue<TreeNode> q = new LinkedBlockingQueue<TreeNode>();
      q.add(root);
      
      while(!q.isEmpty()){
         TreeNode treeNode = q.poll();
         treeNode.accept(this);
         if(treeNode.hasChildren()){
            q.addAll(treeNode.getChildren());
         }
      }      
   }

   public void preOrderTraverse(TreeNode node) {
      node.accept(this);
      if(node.hasChildren()){
         for (TreeNode child : node.getChildren()) {
            preOrderTraverse(child);
         }
      }
   }

   public void postOrderTraverse(TreeNode node) {      
      if(node.hasChildren()){
         for (TreeNode child : node.getChildren()) {
            preOrderTraverse(child);
         }
      }
      node.accept(this);
   } 
}