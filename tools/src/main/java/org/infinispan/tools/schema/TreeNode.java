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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSComplexType;

/**
 * TreeNode of Infinispan configuration
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class TreeNode implements Iterable<TreeNode>, Comparable<TreeNode> {
   private final String name;
   private TreeNode parent;
   private final Set<TreeNode> children = new HashSet<TreeNode>();

   private XSComplexType type;
   private final Set<XSAttributeDecl> attributes = new HashSet<XSAttributeDecl>();
   private Class<?> clazz;

   public TreeNode(String name, TreeNode parent) {
      this.name = name;
      this.parent = parent;
   }

   public TreeNode() {
      this.name = "";
      this.parent = null;
   }

   public String getName() {
      return name;
   }

   public int getDepth() {
      if (getParent() == null) {
         return -1;
      } else {
         return 1 + getParent().getDepth();
      }
   }

   public boolean hasChildren() {
      return !children.isEmpty();
   }
   
   public boolean hasChild(String name) {
      for (TreeNode treeNode : children) {
         if (treeNode.getName().equals(name)) {
            return true;
         }
      }
      return false;
   }

   public TreeNode getParent() {
      return parent;
   }

   public Set<TreeNode> getChildren() {
      return children;
   }
   
   public void detach(){
      parent.getChildren().remove(this);
      parent = null;
   }

   public void accept(TreeWalker tw) {
      tw.visitNode(this);
   }

   public Class<?> getBeanClass() {
      return clazz;
   }

   public void setBeanClass(Class<?> bean) {
      this.clazz = bean;
   }

   public XSComplexType getType() {
      return type;
   }

   public void setType(XSComplexType type) {
      this.type = type;
   }

   public void addAttribute(XSAttributeDecl att) {
      attributes.add(att);
   }

   public Set<XSAttributeDecl> getAttributes() {
      return attributes;
   }

   @Override
   public Iterator<TreeNode> iterator() {
      return new TreeIterator();
   }

   public boolean equals(Object other) {
      if (other == this)
         return true;
      if (!(other instanceof TreeNode))
         return false;
      TreeNode tn = (TreeNode) other;
      return this.parent.name != null && tn.parent != null
               && this.parent.name.equals(tn.parent.name) && this.name.equals(tn.name);
   }
   
   @Override
   public int compareTo(TreeNode o) {       
       return name.compareTo(o.getName());
   }

   public int hashCode() {
      int result = 17;
      result = 31 * result + name.hashCode();
      result = 31 * result + ((parent != null && parent.name != null) ? parent.name.hashCode() : 0);
      return result;
   }

   public String toString() {
      return name;
   }

   private class TreeIterator extends AbstractTreeWalker implements Iterator<TreeNode>,
            Iterable<TreeNode> {

      private List<TreeNode> nodes;
      private Iterator<TreeNode> i;

      private TreeIterator() {
         super();
         nodes = new ArrayList<TreeNode>();
         preOrderTraverse(TreeNode.this);
         Collections.sort(nodes);
         i = nodes.iterator();
      }

      @Override
      public Iterator<TreeNode> iterator() {
         return nodes.iterator();
      }

      @Override
      public void visitNode(TreeNode treeNode) {
         nodes.add(treeNode);
      }

      @Override
      public boolean hasNext() {
         return i.hasNext();
      }

      @Override
      public TreeNode next() {
         return i.next();
      }

      @Override
      public void remove() {
      }
   }
}