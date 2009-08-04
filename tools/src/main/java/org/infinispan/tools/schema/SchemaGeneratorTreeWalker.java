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
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.ConfigurationProperty;
import org.infinispan.config.ConfigurationElement.Cardinality;
import org.infinispan.config.parsing.ConfigurationElementWriter;
import org.infinispan.config.parsing.TreeNode;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * TreeWalker that generates each node of the XML schema
 *
 * @author Vladimir Blagojevic
 * @see SchemaGenerator
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
      Class<?> bean = findBean(beans, treeNode.getName(), treeNode.getParent().getName());
      if (bean == null) {
         log.warn("Did not find bean for node " + treeNode+ ". Should happen only for infinispan node");
         writeInfinispanType();
         return;
      }
           
      ConfigurationElement ce = findConfigurationElementForBean(bean, treeNode.getName(), treeNode.getParent().getName());
      if(ce == null){
         log.warn("Did not find ConfigurationElement for " + treeNode+ ". Verify annotations on all AbstractConfigurationBeans");
         return;
      }
      
      ConfigurationElementWriter writer = null;
      boolean hasCustomWriter = !ce.customWriter().equals(ConfigurationElementWriter.class);
      if(hasCustomWriter){
         try {
         writer = ce.customWriter().newInstance();
         } catch (Exception e1) {
            throw new ConfigurationException("Could not instantiate custom writer ", e1);
         }      
      }
      log.debug("Visiting " + treeNode.getName() + ((hasCustomWriter)?" will use " + writer:""));     
      if (hasCustomWriter) {         
         try {
            writer.process(treeNode, xmldoc);
         } catch (Exception e1) {
            throw new ConfigurationException("Exception while using custom writer ", e1);
         }
      } else {      
         Element complexType = xmldoc.createElement("xs:complexType");
         complexType.setAttribute("name", treeNode.getName() + "TypeIn" + treeNode.getParent().getName());
         createProperty(treeNode, complexType);
         if(treeNode.hasChildren()) {
            boolean sequence = false;
            for(TreeNode child:treeNode.getChildren()){
               ConfigurationElement cce = findConfigurationElement(beans,child.getName(),treeNode.getName());
               if(cce.cardinalityInParent().equals(Cardinality.UNBOUNDED)){
                  sequence = true;
                  break;
               }
            }
            Element allOrSequence = null;
            if(sequence){
               allOrSequence = xmldoc.createElement("xs:sequence");
            } else {
               allOrSequence = xmldoc.createElement("xs:all");
            }
            complexType.appendChild(allOrSequence);
            
            for (TreeNode child : treeNode.getChildren()) {
               ConfigurationElement cce = findConfigurationElement(beans,child.getName(),treeNode.getName());
               Element childElement = xmldoc.createElement("xs:element");
               childElement.setAttribute("name", child.getName());
               childElement.setAttribute("type", "tns:" + child.getName() + "TypeIn" + child.getParent().getName());
               childElement.setAttribute("minOccurs", "0");
               if(cce.cardinalityInParent().equals(Cardinality.UNBOUNDED)){
                  childElement.setAttribute("maxOccurs", "unbounded");     
               } else {
                  childElement.setAttribute("maxOccurs", "1");            
               }      
               //add documentation for this child
               if (cce.description().length() > 0) {
                  addDocumentation(cce.description(), childElement);
               }
               allOrSequence.appendChild(childElement);
            }
            createAttribute(treeNode, complexType);            
         } else { 
            createAttribute(treeNode, complexType);         
         }         
         postProcess(treeNode,complexType);
         xmldoc.getDocumentElement().appendChild(complexType);
      }
   }
   
   protected void postProcess(TreeNode treeNode, Element complexType) {
      
      //dealing with default/namedCache intricacies
      if(treeNode.getName().equals("default")){
         Element element = xmldoc.createElement("xs:attribute");
         element.setAttribute("name", "name");
         element.setAttribute("type", "xs:string");
         complexType.appendChild(element);
      }
   }

   @Override
   public void postTraverseCleanup() {
      //include special property type not visited by TreeWalker
      Element property = xmldoc.createElement("xs:complexType");       
      property.setAttribute("name", "propertyType");
      Element att = xmldoc.createElement("xs:attribute");       
      att.setAttribute("name", "name");
      att.setAttribute("type", "xs:string");
      property.appendChild(att);
      att = xmldoc.createElement("xs:attribute");       
      att.setAttribute("name", "value");
      att.setAttribute("type", "xs:string");
      property.appendChild(att);
      
      xmldoc.getDocumentElement().appendChild(property);            
   }

   private void createAttribute(TreeNode treeNode, Element complexType) {
      Class <?> bean = findBean(beans, treeNode.getName(), treeNode.getParent().getName());
      if(bean == null){
         log.warn("Did not find bean for node " + treeNode + ". Verify annotations on all AbstractConfigurationBeans");
         return;
      }   
      
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
            boolean hasRestriction = a.allowedValues().length >0;
            if(!hasRestriction){
               att.setAttribute("type", "xs:" + type);
            }
            else {
               Element simpleType = xmldoc.createElement("xs:simpleType");
               att.appendChild(simpleType);
               Element restriction = xmldoc.createElement("xs:restriction");
               restriction.setAttribute("base", "xs:" + type);
               simpleType.appendChild(restriction);
               String [] values = a.allowedValues();
               for (String constraint : values) {
                  Element restrictionValue = xmldoc.createElement("xs:enumeration");                     
                  restrictionValue.setAttribute("value", constraint.trim());
                  restriction.appendChild(restrictionValue);                     
               }                  
            }
            //add documentation
            if (a.description().length() > 0) {
               addDocumentation(a.description(), att);
            }
            complexType.appendChild(att);
         }         
      }
   }

   private void addDocumentation(String doco, Element e) {
      Element annotationElement = xmldoc.createElement("xs:annotation");
      e.appendChild(annotationElement);
      Element documentationElement = xmldoc.createElement("xs:documentation");
      documentationElement.setTextContent(doco);
      annotationElement.appendChild(documentationElement);
   }
   
   private void createProperty(TreeNode treeNode, Element complexType) {
      if (treeNode.getParent().getParent() == null)
         return;
      
      Class<?> bean = findBean(beans, treeNode.getName(), treeNode.getParent().getName());
      if (bean == null) {
         log.warn("Did not find bean for node " + treeNode+ ". Try parent, maybe property is there...");
         bean = findBean(beans, treeNode.getParent().getName(), treeNode.getParent().getParent().getName());
         if(bean == null)
            return;
      }    
      
      String createdForParentElement = null;
      for (Method m : bean.getMethods()) {         
         for (ConfigurationProperty c : propertiesElementsOnMethod(m)) {
            boolean property = treeNode.getName().equals(c.parentElement());
            if (property && !c.parentElement().equals(createdForParentElement)) {
               createdForParentElement = c.parentElement();
               Element prop = xmldoc.createElement("xs:sequence");
               Element e = xmldoc.createElement("xs:element");
               prop.appendChild(e);
               e.setAttribute("name", "property");
               e.setAttribute("maxOccurs", "unbounded");
               e.setAttribute("minOccurs", "0");
               e.setAttribute("type", "tns:propertyType");
               complexType.appendChild(prop);
            }
         }         
      }
   }  
   
   private boolean isSetterMethod(Method m) {
      return m.getName().startsWith("set") && m.getParameterTypes().length == 1;
   }
   
   private void writeInfinispanType() {
      Element xsElement = xmldoc.createElement("xs:element");
      xsElement.setAttribute("name", "infinispan");
      xsElement.setAttribute("type", "tns:infinispanTypeIn");
      xmldoc.getDocumentElement().appendChild(xsElement);

      Element complexType = xmldoc.createElement("xs:complexType");
      complexType.setAttribute("name", "infinispanTypeIn");
      Element seq = xmldoc.createElement("xs:sequence");
      complexType.appendChild(seq);

      Element e = xmldoc.createElement("xs:element");
      e.setAttribute("name", "global");
      e.setAttribute("type", "tns:globalTypeIninfinispan");
      e.setAttribute("minOccurs", "0");
      e.setAttribute("maxOccurs", "1");
      seq.appendChild(e);

      e = xmldoc.createElement("xs:element");
      e.setAttribute("name", "default");
      e.setAttribute("type", "tns:defaultTypeIninfinispan");
      e.setAttribute("minOccurs", "0");
      e.setAttribute("maxOccurs", "1");
      seq.appendChild(e);

      e = xmldoc.createElement("xs:element");
      e.setAttribute("name", "namedCache");
      e.setAttribute("type", "tns:defaultTypeIninfinispan");
      e.setAttribute("minOccurs", "0");
      e.setAttribute("maxOccurs", "unbounded");
      seq.appendChild(e);

      xmldoc.getDocumentElement().appendChild(complexType);
   }
}