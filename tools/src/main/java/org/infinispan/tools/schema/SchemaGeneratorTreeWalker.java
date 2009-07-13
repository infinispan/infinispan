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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationElements;
import org.infinispan.config.ConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * TreeWalker that generates each node of the XML schema
 *
 * @author Vladimir Blagojevic
 * @see SchemaGenerator
 * @version $Id$
 * @since 4.0
 */
public class SchemaGeneratorTreeWalker extends ConfigurationTreeWalker{
   
   Document xmldoc;
   private List<Class<?>> beans;

   public SchemaGeneratorTreeWalker(Document xmldoc, List<Class<?>> beans) {
      super();
      this.xmldoc = xmldoc;
      this.beans = beans;
   }
   
   public void visitNode(TreeNode treeNode) {
      
      Element complexType = xmldoc.createElement("xs:complexType");
      complexType.setAttribute("name", treeNode.getName() + "Type");
      if(treeNode.hasChildren()) {
         Element all = xmldoc.createElement("xs:all");
         complexType.appendChild(all);
         Set<TreeNode> children = treeNode.getChildren();
         for (TreeNode child : children) {
            Element childElement = xmldoc.createElement("xs:element");
            childElement.setAttribute("name", child.getName());
            childElement.setAttribute("type", "tns:" + child.getName() + "Type");
            //childElement.setAttribute("minOccurs", "0");
            //childElement.setAttribute("maxOccurs", "1");            
            all.appendChild(childElement);
         }
      } else { 
         Class <?> bean = findBean(beans, treeNode.getName(), treeNode.getParent().getName());
         for (Method m : bean.getMethods()) {
            ConfigurationAttribute a = m.getAnnotation(ConfigurationAttribute.class);
            boolean childElement = a != null && a.containingElement().equals(treeNode.getName());           
            if (childElement) {
               String type = "";
               if (isSetterMethod(m)) {
                  type = m.getParameterTypes()[0].getSimpleName();
                  type = type.toLowerCase();
               }                            
               Element att = xmldoc.createElement("xs:attribute");
               att.setAttribute("name", a.name());
               att.setAttribute("type", "xs:" + type);
               complexType.appendChild(att);
            }
         }         
      }
      xmldoc.getDocumentElement().appendChild(complexType);
   }
   
   private Class<?> findBean(List<Class<?>> b, String name, String parentName) throws ConfigurationException {
      
      if (parentName.equals("namedCache"))
         parentName = "default";
      for (Class<?> clazz : b) {
         ConfigurationElements elements = clazz.getAnnotation(ConfigurationElements.class);
         try {
            if (elements != null) {
               for (ConfigurationElement ce : elements.elements()) {
                  if (ce.name().equals(name) && ce.parent().equals(parentName)) {
                     return clazz;
                  }
               }
            } else {
               ConfigurationElement ce = clazz.getAnnotation(ConfigurationElement.class);
               if (ce != null && (ce.name().equals(name) && ce.parent().equals(parentName))) {
                  return clazz;
               }
            }
         } catch (Exception e1) {
            throw new ConfigurationException("Could not instantiate class " + clazz, e1);
         }
      }
      return null;
   }
   
   
   private boolean isSetterMethod(Method m) {
      return m.getName().startsWith("set") && m.getParameterTypes().length == 1;
   }
}
