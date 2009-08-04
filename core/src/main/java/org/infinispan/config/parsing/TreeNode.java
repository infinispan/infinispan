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
package org.infinispan.config.parsing;

import java.util.HashSet;
import java.util.Set;

/**
 * TreeNode of Infinispan configuration
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class TreeNode {
   private final String name;
   private final TreeNode parent;
   private final int depth;
   private final Set<TreeNode> children = new HashSet<TreeNode>();
   
   public TreeNode(String name, TreeNode parent, int depth) {
      this.name = name;
      this.parent = parent;
      this.depth = depth;
   }   
   
   public TreeNode() {
      this.name="";
      this.parent=null;
      this.depth = -1; // :)
   }

   public String getName() {
      return name;
   }
      
   public int getDepth() {
      return depth;
   }

   public boolean hasChildren(){
      return !children.isEmpty();
   }

   public TreeNode getParent() {
      return parent;
   }
   
   public Set<TreeNode> getChildren() {
      return children;
   }
   
   public void accept(TreeWalker tw) {
      tw.visitNode(this);                     
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

   public int hashCode() {
      int result = 17;
      result = 31 * result + name.hashCode();
      result = 31 * result
               + ((parent != null && parent.name != null) ? parent.name.hashCode() : 0);
      return result;
   }
   
   public String toString() {
      return name;
   }
}