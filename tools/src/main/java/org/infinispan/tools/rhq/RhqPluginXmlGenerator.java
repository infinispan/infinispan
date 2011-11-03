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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.infinispan.tools.BeanConventions;
import org.infinispan.util.ClassFinder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;
import org.rhq.helpers.pluginGen.PluginGen;
import org.rhq.helpers.pluginGen.Props;
import org.rhq.helpers.pluginGen.ResourceCategory;
import org.rhq.helpers.pluginGen.Props.MetricProps;
import org.rhq.helpers.pluginGen.Props.OperationProps;
import org.rhq.helpers.pluginGen.Props.SimpleProperty;
import org.rhq.helpers.pluginGen.Props.TypeKey;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.RootDoc;

/**
 * RhqPluginDoclet.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class RhqPluginXmlGenerator {
   private static final Log log = LogFactory.getLog(RhqPluginXmlGenerator.class);
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
            debug("Add as global class " + clazz);
            globalClasses.add(clazz);
         } else {
            debug("Add as named cache class " + clazz);
            namedCacheClasses.add(clazz);
         }
      }
      
      // Init the Javassist class pool.
      classPool = ClassPool.getDefault();
      classPool.insertClassPath(new ClassClassPath(RhqPluginXmlGenerator.class));
      PluginGen pg = new PluginGen();
      
      Props root = new Props();
      root.setPluginName("Infinispan");
      root.setPluginDescription("Supports management and monitoring of Infinispan");
      root.setName("Infinispan Cache Manager");
      root.setPkg("org.infinispan.rhq");
      root.setDependsOnJmxPlugin(true);
      root.setDiscoveryClass("CacheManagerDiscovery");
      root.setComponentClass("CacheManagerComponent");
      root.setSingleton(false);
      root.setCategory(ResourceCategory.SERVICE);
      Set<TypeKey> servers = new HashSet<TypeKey>();
      servers.add(new TypeKey("JMX Server", "JMX"));
      servers.add(new TypeKey("JBossAS Server", "JBossAS"));
      servers.add(new TypeKey("JBossAS Server", "JBossAS5"));
      root.setRunsInsides(servers);
      populateMetricsAndOperations(globalClasses, root, false);

      Props cache = new Props();
      cache.setName("Infinispan Cache");
      cache.setPkg("org.infinispan.rhq");
      cache.setDependsOnJmxPlugin(true);
      cache.setDiscoveryClass("CacheDiscovery");
      cache.setComponentClass("CacheComponent");
      cache.setSingleton(false);
      cache.setCategory(ResourceCategory.SERVICE);
      populateMetricsAndOperations(namedCacheClasses, cache, true);
      
      root.getChildren().add(cache);

      String metaInfDir = "../../../src/main/resources/META-INF";
      new File(metaInfDir).mkdirs();
      String targetMetaInfDir = "../../../target/classes/META-INF";
      new File(targetMetaInfDir).mkdirs();

      pg.createFile(root, "descriptor", "rhq-plugin.xml", metaInfDir);
      copyFile(new File(metaInfDir + "/rhq-plugin.xml"), new File(targetMetaInfDir + "/rhq-plugin.xml"));
      
      return true;
   }

   private static void copyFile(File in, File out) throws IOException {
      FileChannel inCh = new FileInputStream(in).getChannel();
      FileChannel outCh = new FileOutputStream(out).getChannel();
      try {
         inCh.transferTo(0, inCh.size(), outCh);
      } catch (IOException e) {
         throw e;
      } finally {
         if (inCh != null) inCh.close();
         if (outCh != null) outCh.close();
      }
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
   
   private static void populateMetricsAndOperations(List<Class<?>> classes,
            Props props, boolean withNamePrefix) throws Exception {
      props.setHasOperations(true);
      props.setHasMetrics(true);
      for (Class<?> clazz : classes) {
         MBean mbean = clazz.getAnnotation(MBean.class);
         String prefix = withNamePrefix ? mbean.objectName() + '.' : "";
         CtClass ctClass = classPool.get(clazz.getName());

         CtMethod[] ctMethods = ctClass.getMethods();
         for (CtMethod ctMethod : ctMethods) {
            ManagedAttribute managedAttr = (ManagedAttribute)
                  ctMethod.getAnnotation(ManagedAttribute.class);
            ManagedOperation managedOp = (ManagedOperation)
                  ctMethod.getAnnotation(ManagedOperation.class);

            Metric rhqMetric = (Metric) ctMethod.getAnnotation(Metric.class);
            if (rhqMetric != null) {
               debug("Metric annotation found " + rhqMetric);
               // Property and description resolution are the reason why annotation scanning is done here.
               // These two fields are calculated from either the method name or the Managed* annotations,
               // and so, only the infinispan side knows about that.
               String property = prefix + getPropertyFromBeanConvention(ctMethod);
               if (!rhqMetric.property().isEmpty()) {
                  property = prefix + rhqMetric.property();
               }
               MetricProps metric = new MetricProps(property);
               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + rhqMetric.displayName() : rhqMetric.displayName();
               metric.setDisplayName(displayName);
               metric.setDisplayType(rhqMetric.displayType());
               metric.setDataType(rhqMetric.dataType());
               metric.setUnits(rhqMetric.units());
               if (managedAttr != null) {
                  debug("Metric has ManagedAttribute annotation " + managedAttr);
                  metric.setDescription(managedAttr.description());
               } else if (managedOp != null) {
                  debug("Metric has ManagedOperation annotation " + managedOp);
                  metric.setDescription(managedOp.description());
               } else {
                  log.debug("Metric has no managed annotations, so take the description from the display name.");
                  metric.setDescription(rhqMetric.displayName());
               }
               props.getMetrics().add(metric);
            }
            
            Operation rhqOperation = (Operation) ctMethod.getAnnotation(Operation.class);
            if (rhqOperation != null) {
               debug("Operation annotation found " + rhqOperation);
               String name;
               if (!rhqOperation.name().isEmpty()) {
                  name = prefix + rhqOperation.name();
               } else {
                  name = prefix + ctMethod.getName();
               }
               OperationProps operation = new OperationProps(name);
               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + rhqOperation.displayName() : rhqOperation.displayName();
               operation.setDisplayName(displayName);
               if (managedAttr != null) {
                  debug("Operation has ManagedAttribute annotation " + managedAttr);
                  operation.setDescription(managedAttr.description());
               } else if (managedOp != null) {
                  debug("Operation has ManagedOperation annotation " + managedOp);
                  operation.setDescription(managedOp.description());
               } else {
                  debug("Operation has no managed annotations, so take the description from the display name.");
                  operation.setDescription(rhqOperation.displayName());
               }
               
               Object[][] paramAnnotations = ctMethod.getParameterAnnotations();
               int i = 0;
               for (Object[] paramAnnotationsInEach : paramAnnotations) {
                  boolean hadParameter = false;
                  for (Object annot : paramAnnotationsInEach) {
                     debug("Parameter annotation " + annot);
                     if (annot instanceof Parameter) {
                        Parameter param = (Parameter) annot;
                        SimpleProperty prop = new SimpleProperty(param.name());
                        prop.setDescription(param.description());
                        operation.getParams().add(prop);
                        hadParameter = true;
                     }
                  }
                  if (!hadParameter) {
                     operation.getParams().add(new SimpleProperty("p" + i++));
                  }
               }
               CtClass returnType = ctMethod.getReturnType();
               if (!returnType.equals(CtClass.voidType)) {
                  if (!returnType.equals(Void.TYPE)) {
                     SimpleProperty prop = new SimpleProperty("operationResult");
                     operation.setResult(prop);
                  }
               }
               props.getOperations().add(operation);
            }
         }

         CtField[] ctFields = ctClass.getDeclaredFields();
         for (CtField ctField : ctFields) {
            debug("Inspecting field " + ctField);

            Metric rhqMetric = (Metric)ctField.getAnnotation(Metric.class);
            if (rhqMetric != null) {
              debug("Field " + ctField + " contains Metric annotation " + rhqMetric);
              String property;
               if (!rhqMetric.property().isEmpty()) {
                  property = prefix + rhqMetric.property();
               } else {
                  property = prefix + getPropertyFromBeanConvention(ctField);
               }
               MetricProps metric = new MetricProps(property);
               String displayName = withNamePrefix ? "[" + mbean.objectName() + "] " + rhqMetric.displayName() : rhqMetric.displayName();
               metric.setDisplayName(displayName);
               metric.setDisplayType(rhqMetric.displayType());
               metric.setDataType(rhqMetric.dataType());
               metric.setUnits(rhqMetric.units());
               ManagedAttribute managedAttr = (ManagedAttribute)
                     ctField.getAnnotation(ManagedAttribute.class);
               if (managedAttr != null) {
                  debug("Metric has ManagedAttribute annotation " + managedAttr);
                  metric.setDescription(managedAttr.description());
               } else {
                  log.debug("Metric has no managed annotations, so take the description from the display name.");
                  metric.setDescription(rhqMetric.displayName());
               }
               props.getMetrics().add(metric);
            }
         }
        
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

   private static void debug(Object o) {
//      if (log.isDebugEnabled()) log.debug(o);
//      System.out.println(o);
   }

}
