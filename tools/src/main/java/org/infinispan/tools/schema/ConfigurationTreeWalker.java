/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.tools.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.parsing.RootElementBuilder;
import org.infinispan.config.parsing.TreeNode;
import org.infinispan.config.parsing.TreeWalker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * TreeWalker abstract super class that should be extended for a particular tool
 *
 * @author Vladimir Blagojevic
 * @see SchemaGeneratorTreeWalker
 * @see XMLTreeOutputWalker
 * @version $Id$
 * @since 4.0
 */
public abstract class ConfigurationTreeWalker implements TreeWalker{

   protected final Log log;
   
   public ConfigurationTreeWalker() {
      super();
      log = LogFactory.getLog(getClass());
   }

   public TreeNode constructTreeFromBeans(List<Class<?>>configBeans) {
      List<ConfigurationElement> lce = new ArrayList<ConfigurationElement>(7);
      for (Class<?> clazz : configBeans) {
         ConfigurationElement ces[] = null;
         ConfigurationElements configurationElements = clazz.getAnnotation(ConfigurationElements.class);
         ConfigurationElement configurationElement = clazz.getAnnotation(ConfigurationElement.class);
   
         if (configurationElement != null && configurationElements == null) {
            ces = new ConfigurationElement[]{configurationElement};
         }
         if (configurationElements != null && configurationElement == null) {
            ces = configurationElements.elements();
         }
         if(ces != null){
            lce.addAll(Arrays.asList(ces));
         }
      }
      TreeNode root = new TreeNode("infinispan",new TreeNode(),0);      
      makeTree(lce,root, 1);
      return root;
   }

   private void makeTree(List<ConfigurationElement> lce, TreeNode tn, int currentDepth) {
      for (ConfigurationElement ce : lce) {
         if(ce.parent().equals(tn.getName())){
            TreeNode child = new TreeNode(ce.name(),tn,currentDepth);
            tn.getChildren().add(child);
            makeTree(lce,child,(currentDepth+1));
         }
      }
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
   
   public ConfigurationElement[] configurationElementsOnBean(Class<?> clazz) {
      ConfigurationElements configurationElements = clazz.getAnnotation(ConfigurationElements.class);
      ConfigurationElement configurationElement = clazz.getAnnotation(ConfigurationElement.class);
      ConfigurationElement ces [] = new ConfigurationElement[0];
      if (configurationElement != null && configurationElements == null) {
         ces = new ConfigurationElement[]{configurationElement};
      }
      if (configurationElements != null && configurationElement == null) {
         ces = configurationElements.elements();
      }
      return ces;
   }
}