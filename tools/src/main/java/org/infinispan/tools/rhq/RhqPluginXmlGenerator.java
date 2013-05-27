/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.tools.rhq;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.util.ClassFinder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.RootDoc;

/**
 * RhqPluginDoclet.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class RhqPluginXmlGenerator {
   private static final String URN_XMLNS_RHQ_CONFIGURATION = "urn:xmlns:rhq-configuration";
   private static ClassPool classPool;
   private static String cp;

   public static void main(String[] args) throws Exception {
      cp = System.getProperty("java.class.path");
      start(null);
   }

   public static boolean validOptions(String options[][], DocErrorReporter reporter) {
      for (String[] option : options) {
         if (option[0].equals("-classpath"))
            cp = option[1];
      }
      return true;
   }

   public static boolean start(RootDoc rootDoc) throws Exception {
      List<Class<?>> mbeanIspnClasses = getMBeanClasses();
      List<Class<?>> globalClasses = new ArrayList<Class<?>>();
      List<Class<?>> namedCacheClasses = new ArrayList<Class<?>>();
      for (Class<?> clazz : mbeanIspnClasses) {
         Scope scope = clazz.getAnnotation(Scope.class);
         if (scope != null && scope.value() == Scopes.GLOBAL) {
            globalClasses.add(clazz);
         } else {
            namedCacheClasses.add(clazz);
         }
      }

      // Init the Javassist class pool.
      classPool = ClassPool.getDefault();
      classPool.insertClassPath(new ClassClassPath(RhqPluginXmlGenerator.class));
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setNamespaceAware(true);
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.newDocument();
      Element root = doc.createElement("plugin");
      root.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:c", URN_XMLNS_RHQ_CONFIGURATION);
      doc.appendChild(root);

      populateMetricsAndOperations(globalClasses, root, "cacheManager", false);

      populateMetricsAndOperations(namedCacheClasses, root, "cache", true);

      String targetMetaInfDir = "../../../target/classes/META-INF";
      new File(targetMetaInfDir).mkdirs();

      TransformerFactory tf = TransformerFactory.newInstance();
      StreamSource xslt = new StreamSource(RhqPluginXmlGenerator.class.getResourceAsStream("/META-INF/rhq-plugin.xslt"));
      Transformer transformer = tf.newTransformer(xslt);
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      Result output = new StreamResult(new File(targetMetaInfDir + "/rhq-plugin.xml"));
      transformer.transform(new DOMSource(doc), output);

      return true;
   }

   private static List<Class<?>> getMBeanClasses() throws IOException {
      try {
         return ClassFinder.withAnnotationDeclared(ClassFinder.infinispanClasses(cp), MBean.class);
      } catch (Exception e) {
         IOException ioe = new IOException("Unable to get Infinispan classes");
         ioe.initCause(e);
         throw ioe;
      }
   }

   private static void populateMetricsAndOperations(List<Class<?>> classes, Element root, String parentName, boolean withNamePrefix) throws Exception {
      Set<String> uniqueOperations = new HashSet<String>();
      Document doc = root.getOwnerDocument();
      Element parent = doc.createElement(parentName);
      for (Class<?> clazz : classes) {
         MBean mbean = clazz.getAnnotation(MBean.class);
         String prefix = withNamePrefix ? mbean.objectName() + '.' : "";
         CtClass ctClass = classPool.get(clazz.getName());

         CtMethod[] ctMethods = ctClass.getMethods();
         for (CtMethod ctMethod : ctMethods) {
            ManagedAttribute managedAttr = (ManagedAttribute) ctMethod.getAnnotation(ManagedAttribute.class);
            ManagedOperation managedOp = (ManagedOperation) ctMethod.getAnnotation(ManagedOperation.class);

            if (managedAttr != null) {
               String property = prefix + getPropertyFromBeanConvention(ctMethod);

               String attrDisplayName = managedAttr.displayName();
               if (attrDisplayName.length() == 0) {
                  throw new RuntimeException("Missing displayName on: " + property);
               }
               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + attrDisplayName : attrDisplayName;
               validateDisplayName(displayName);

               Element metric = doc.createElement("metric");
               metric.setAttribute("property", property);
               metric.setAttribute("displayName", displayName);
               metric.setAttribute("displayType", managedAttr.displayType().toString());
               metric.setAttribute("dataType", managedAttr.dataType().toString());
               metric.setAttribute("units", managedAttr.units().toString());
               metric.setAttribute("description", managedAttr.description());

               parent.appendChild(metric);
            }

            if (managedOp != null) {
               String name;
               if (!managedOp.name().isEmpty()) {
                  name = prefix + managedOp.name();
               } else {
                  name = prefix + ctMethod.getName();
               }

               Object[][] paramAnnotations = ctMethod.getParameterAnnotations();
               Element parameters = doc.createElement("parameters");
               for (Object[] paramAnnotationsInEach : paramAnnotations) {
                  boolean annotatedParameter = false;
                  for (Object annot : paramAnnotationsInEach) {
                     if (annot instanceof Parameter) {
                        Parameter param = (Parameter) annot;
                        Element prop = doc.createElementNS(URN_XMLNS_RHQ_CONFIGURATION, "simple-property");
                        name += "|" + param.name();
                        prop.setAttribute("name", param.name());
                        prop.setAttribute("description", param.description());
                        // default type from RHQ is String but sometimes we need (numbers) integer or long
                        if (!param.type().equals("")) prop.setAttribute("type", param.type());
                        parameters.appendChild(prop);
                        annotatedParameter = true;
                     }
                  }
                  if (!annotatedParameter) {
                     throw new RuntimeException("Duplicate operation name: " + name);
                  }
               }

               if (uniqueOperations.contains(name)) {
                  throw new RuntimeException("Duplicate operation name: " + name);
               }
               uniqueOperations.add(name);

               String opDisplayName = managedOp.displayName();
               if (opDisplayName.length() == 0) {
                  throw new RuntimeException("Missing displayName on: " + name);
               }
               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + opDisplayName : opDisplayName;
               validateDisplayName(displayName);

               Element operation = doc.createElement("operation");
               operation.setAttribute("name", name);
               operation.setAttribute("displayName", displayName);
               if (managedAttr != null) {
                  operation.setAttribute("description", managedAttr.description());
               } else {
                  operation.setAttribute("description", managedOp.description());
               }

               if(parameters.hasChildNodes()) {
                  operation.appendChild(parameters);
               }
               CtClass returnType = ctMethod.getReturnType();
               if (!returnType.equals(CtClass.voidType) && !returnType.equals(Void.TYPE)) {
                  Element results = doc.createElement("results");
                  Element prop = doc.createElementNS(URN_XMLNS_RHQ_CONFIGURATION, "simple-property");
                  prop.setAttribute("name", "operationResult");
                  results.appendChild(prop);
                  operation.appendChild(results);
               }
               parent.appendChild(operation);
            }
         }

         CtField[] ctFields = ctClass.getDeclaredFields();
         for (CtField ctField : ctFields) {
            ManagedAttribute managedAttr = (ManagedAttribute) ctField.getAnnotation(ManagedAttribute.class);
            if (managedAttr != null) {
               String property = prefix + getPropertyFromBeanConvention(ctField);
               Element metric = doc.createElement("metric");
               metric.setAttribute("property", property);

               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + managedAttr.displayName() : managedAttr.displayName();
               validateDisplayName(displayName);
               metric.setAttribute("property", property);
               metric.setAttribute("displayName", displayName);
               metric.setAttribute("displayType", managedAttr.displayType().toString());
               metric.setAttribute("dataType", managedAttr.dataType().toString());
               metric.setAttribute("units", managedAttr.units().toString());
               metric.setAttribute("description", managedAttr.description());
               parent.appendChild(metric);
            }
         }

      }
      root.appendChild(parent);
   }

   private static void validateDisplayName(String displayName) {
      if (displayName.length() > 100) {
         throw new RuntimeException("Display name too long (max 100 chars): " + displayName);
      }
   }

   private static String getPropertyFromBeanConvention(CtMethod ctMethod) {
      String getterOrSetter = ctMethod.getName();
      if (getterOrSetter.startsWith("get") || getterOrSetter.startsWith("set")) {
         String withoutGet = getterOrSetter.substring(4);
         // not specifically Bean convention, but this is what is bound in JMX.
         return Character.toUpperCase(getterOrSetter.charAt(3)) + withoutGet;
      } else if (getterOrSetter.startsWith("is")) {
         String withoutIs = getterOrSetter.substring(3);
         return Character.toUpperCase(getterOrSetter.charAt(2)) + withoutIs;
      }
      return getterOrSetter;
   }

   private static String getPropertyFromBeanConvention(CtField ctField) {
      String fieldName = ctField.getName();
      String withoutFirstChar = fieldName.substring(1);
      return Character.toUpperCase(fieldName.charAt(0)) + withoutFirstChar;
   }
}
