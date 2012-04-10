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
package org.infinispan.tools.doclet.config;

import org.infinispan.tools.schema.AbstractTreeWalker;
import org.infinispan.tools.schema.TreeNode;

/**
 * TreeWalker that generates XML pretty print of the configuration tree
 * 
 * @author Vladimir Blagojevic
 * @see ConfigDoclet
 * @since 4.0
 */
public class XMLTreeOutputWalker extends AbstractTreeWalker {

   private final StringBuilder sb;
   private static final String IDENT = "  ";

   public XMLTreeOutputWalker(StringBuilder sb) {
      super();
      this.sb = sb;
   }

   @Override
   public void visitNode(TreeNode treeNode) {
      String ident = "";
      for (int i = 0; i <= treeNode.getDepth(); i++)
         ident += IDENT;

      sb.append(ident + "&lt;<a href=\"" + "#ce_" + treeNode.getParent().getName() + "_"
               + treeNode.getName() + "\">" + treeNode.getName() + "</a>&gt;" + "\n");

   }

   public TreeNode findNode(TreeNode tn, String name, String parent) {
      TreeNode result = null;
      if (tn.getName().equals(name) && tn.getParent() != null
               && tn.getParent().getName().equals(parent)) {
         result = tn;
      } else {
         for (TreeNode child : tn.getChildren()) {
            result = findNode(child, name, parent);
            if (result != null)
               break;
         }
      }
      return result;
   }
}
