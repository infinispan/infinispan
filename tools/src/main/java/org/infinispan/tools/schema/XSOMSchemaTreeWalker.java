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

import java.util.Iterator;

import com.sun.xml.xsom.XSAnnotation;
import com.sun.xml.xsom.XSAttGroupDecl;
import com.sun.xml.xsom.XSAttributeDecl;
import com.sun.xml.xsom.XSAttributeUse;
import com.sun.xml.xsom.XSComplexType;
import com.sun.xml.xsom.XSContentType;
import com.sun.xml.xsom.XSElementDecl;
import com.sun.xml.xsom.XSFacet;
import com.sun.xml.xsom.XSIdentityConstraint;
import com.sun.xml.xsom.XSListSimpleType;
import com.sun.xml.xsom.XSModelGroup;
import com.sun.xml.xsom.XSModelGroupDecl;
import com.sun.xml.xsom.XSNotation;
import com.sun.xml.xsom.XSParticle;
import com.sun.xml.xsom.XSRestrictionSimpleType;
import com.sun.xml.xsom.XSSchema;
import com.sun.xml.xsom.XSSimpleType;
import com.sun.xml.xsom.XSType;
import com.sun.xml.xsom.XSUnionSimpleType;
import com.sun.xml.xsom.XSWildcard;
import com.sun.xml.xsom.XSXPath;
import com.sun.xml.xsom.visitor.XSSimpleTypeVisitor;
import com.sun.xml.xsom.visitor.XSTermVisitor;
import com.sun.xml.xsom.visitor.XSVisitor;

/**
 * XSOMSchemaTreeWalker traverses XML schema and builds a tree with elements we need out of that schema
 *
 * @author Vladimir Blagojevic
 * @see ConfigHtmlGenerator
 * @since 4.0
 */
public class XSOMSchemaTreeWalker implements XSVisitor {
   private XSSchema schema;
   private TreeNode root;
   private TreeNode currentNode;

   public XSOMSchemaTreeWalker(XSSchema schema, String rootName) {
      super();
      this.schema = schema;
      XSElementDecl decl = schema.getElementDecl(rootName);
      XSComplexType type = schema.getComplexType(decl.getType().getName());
      root = new TreeNode("infinispan", new TreeNode());
      root.setType(type);
      currentNode = root;
      complexType(type);
   }

   public TreeNode getRoot() {
      return root;
   }

   @Override
   public void empty(XSContentType empty) {
   }

   @Override
   public void particle(XSParticle part) {
      int i;

      StringBuilder buf = new StringBuilder();
      i = part.getMaxOccurs();
      if (i == XSParticle.UNBOUNDED) {
         buf.append(" maxOccurs=\"unbounded\"");
      } else {
         if (i != 1) {
            buf.append(" maxOccurs=\"" + i + "\"");
         }
      }

      i = part.getMinOccurs();
      if (i != 1) {
         buf.append(" minOccurs=\"" + i + "\"");
      }

      part.getTerm().visit(new XSTermVisitor() {
         @Override
         public void elementDecl(XSElementDecl decl) {
            if (decl.isLocal()) {
               XSOMSchemaTreeWalker.this.elementDecl(decl);
            } else {
               // reference, don't care
            }
         }

         @Override
         public void modelGroupDecl(XSModelGroupDecl decl) {
         }

         @Override
         public void modelGroup(XSModelGroup group) {
            final int len = group.getSize();
            for (int i = 0; i < len; i++) {
               particle(group.getChild(i));
            }
         }

         @Override
         public void wildcard(XSWildcard wc) {
         }
      });
   }

   @Override
   public void simpleType(XSSimpleType simpleType) {
      simpleType.visit(new XSSimpleTypeVisitor() {

         @Override
         public void listSimpleType(XSListSimpleType type) {
         }

         @Override
         public void restrictionSimpleType(XSRestrictionSimpleType type) {
            //XSSimpleType baseType = type.getSimpleBaseType();
            Iterator<?> itr = type.iterateDeclaredFacets();
            while (itr.hasNext()) {
               facet((XSFacet) itr.next());
            }
         }

         @Override
         public void unionSimpleType(XSUnionSimpleType type) {
         }
      });
   }

   @Override
   public void annotation(XSAnnotation ann) {

   }

   @Override
   public void attGroupDecl(XSAttGroupDecl decl) {

   }

   @Override
   public void attributeDecl(XSAttributeDecl decl) {
      // visitAttribute(decl);
   }

   @Override
   public void attributeUse(XSAttributeUse use) {
      XSAttributeDecl decl = use.getDecl();

      if (decl.isLocal()) {
         // this is anonymous attribute use
         visitAttribute(decl);
      }
   }

   private void visitAttribute(XSAttributeDecl decl) {
      XSSimpleType type = decl.getType();
      //System.out.println("Visiting attribute " + decl.getName() + ":" + type.getName());
      currentNode.addAttribute(decl);
      if (schema.getSimpleType(type.getName()) != null)
         simpleType(type);
   }

   private void dumpComplexTypeAttribute(XSComplexType type) {
      Iterator<?> itr = type.iterateDeclaredAttributeUses();
      while (itr.hasNext()) {
         attributeUse((XSAttributeUse) itr.next());
      }
   }

   @Override
   public void complexType(XSComplexType type) {
      dumpComplexTypeAttribute(type);
      if (type.getDerivationMethod() == XSType.RESTRICTION) {
         type.getContentType().visit(this);
      } else {
         XSType baseType = type.getBaseType();
         String name = baseType.getName();
         XSComplexType parentType = schema.getComplexType(name);
         complexType(parentType);
         type.getExplicitContent().visit(this);
      }
   }

   @Override
   public void facet(XSFacet facet) {
      //System.out.println(facet.getName() + ":" + facet.getValue());
   }

   @Override
   public void identityConstraint(XSIdentityConstraint decl) {
   }

   @Override
   public void notation(XSNotation notation) {
   }

   @Override
   public void schema(XSSchema schema) {
   }

   @Override
   public void xpath(XSXPath xp) {
   }

   @Override
   public void elementDecl(XSElementDecl decl) {
      XSComplexType type = schema.getComplexType(decl.getType().getName());
      if (!decl.isAbstract()) {
         TreeNode n = new TreeNode(decl.getName(), currentNode);
         /*System.out.println("Created node " + n.getName() + ", parent is "
                  + n.getParent().getName() + " depth is " + n.getDepth());*/
         currentNode.getChildren().add(n);
         currentNode = n;
         currentNode.setType(type);
         complexType(type);
         currentNode = n.getParent();
      }
   }

   @Override
   public void modelGroup(XSModelGroup group) {
   }

   @Override
   public void modelGroupDecl(XSModelGroupDecl decl) {
   }

   @Override
   public void wildcard(XSWildcard wc) {
   }
}