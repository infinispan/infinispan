/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.jmx;

import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;
import javax.management.ServiceNotFoundException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class was entirely copied from JGroups 2.7 (same name there). Couldn't simply reuse it
 * because JGroups does not ship with MBean, ManagedAttribute and ManagedOperation. Once JGroups
 * will ship these classes, the code can be dynamically reused from there.
 * <p/>
 * The original JGroup's ResourceDMBean logic has been modified so that {@link #invoke()} method checks
 * whether the operation called has been exposed as a {@link ManagedOperation}, otherwise the call
 * fails. JGroups deviated from this logic on purpose because they liked the fact that you could expose
 * all class methods by simply annotating class with {@link MBean} annotation.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ResourceDMBean implements DynamicMBean {
   private static final Class<?>[] primitives = {int.class, byte.class, short.class, long.class,
           float.class, double.class, boolean.class, char.class};

   private static final String MBEAN_DESCRITION = "Dynamic MBean Description";

   private final Log log = LogFactory.getLog(ResourceDMBean.class);
   private final Object obj;
   private String description = "";

   private final MBeanAttributeInfo[] attrInfo;
   private final MBeanOperationInfo[] opInfos;

   private final HashMap<String, AttributeEntry> atts = new HashMap<String, AttributeEntry>();
   private final List<MBeanOperationInfo> ops = new ArrayList<MBeanOperationInfo>();

   public ResourceDMBean(Object instance) {

      if (instance == null)
         throw new NullPointerException("Cannot make an MBean wrapper for null instance");

      this.obj = instance;
      findDescription();
      findFields();
      findMethods();

      attrInfo = new MBeanAttributeInfo[atts.size()];
      int i = 0;

      MBeanAttributeInfo info;
      for (AttributeEntry entry : atts.values()) {
         info = entry.getInfo();
         attrInfo[i++] = info;
         if (log.isInfoEnabled()) {
            log.trace("Attribute " + info.getName() + "[r=" + info.isReadable() + ",w="
                    + info.isWritable() + ",is=" + info.isIs() + ",type=" + info.getType() + "]");
         }
      }

      opInfos = new MBeanOperationInfo[ops.size()];
      ops.toArray(opInfos);

      if (log.isTraceEnabled()) {
         if (!ops.isEmpty())
            log.trace("Operations are:");
         for (MBeanOperationInfo op : opInfos) {
            log.trace("Operation " + op.getReturnType() + " " + op.getName());
         }
      }
   }

   Object getObject() {
      return obj;
   }

   private synchronized void findDescription() {
      MBean mbean = getObject().getClass().getAnnotation(MBean.class);
      if (mbean != null && mbean.description() != null && mbean.description().trim().length() > 0) {
         description = mbean.description();
         if (log.isDebugEnabled()) {
            log.debug("@MBean description set - " + mbean.description());
         }
         MBeanAttributeInfo info = new MBeanAttributeInfo(MBEAN_DESCRITION, "java.lang.String",
                 "@MBean description", true, false, false);
         try {
            atts.put(MBEAN_DESCRITION, new FieldAttributeEntry(info, getClass().getDeclaredField(
                    "description")));
         } catch (NoSuchFieldException e) {
            // this should not happen unless somebody removes description field
            log.warn("Could not reflect field description of this class. Was it removed?");
         }
      }
   }

   public synchronized MBeanInfo getMBeanInfo() {

      return new MBeanInfo(getObject().getClass().getCanonicalName(), description, attrInfo, null,
              opInfos, null);
   }

   public synchronized Object getAttribute(String name) throws AttributeNotFoundException {
      if (name == null || name.length() == 0)
         throw new NullPointerException("Invalid attribute requested " + name);

      Attribute attr = getNamedAttribute(name);
      if (attr == null) {
         throw new AttributeNotFoundException("Unknown attribute '" + name
                 + "'. Known attributes names are: " + atts.keySet());
      }
      return attr.getValue();
   }

   public synchronized void setAttribute(Attribute attribute) {
      if (attribute == null || attribute.getName() == null)
         throw new NullPointerException("Invalid attribute requested " + attribute);

      setNamedAttribute(attribute);
   }

   public synchronized AttributeList getAttributes(String[] names) {
      AttributeList al = new AttributeList();
      for (String name : names) {
         Attribute attr = getNamedAttribute(name);
         if (attr != null) {
            al.add(attr);
         } else {
            log.warn("Did not find attribute " + name);
         }
      }
      return al;
   }

   public synchronized AttributeList setAttributes(AttributeList list) {
      AttributeList results = new AttributeList();
      for (Object aList : list) {
         Attribute attr = (Attribute) aList;

         if (setNamedAttribute(attr)) {
            results.add(attr);
         } else {
            if (log.isWarnEnabled()) {
               log.warn("Failed to update attribute name " + attr.getName() + " with value "
                       + attr.getValue());
            }
         }
      }
      return results;
   }

   public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
           ReflectionException {
      if (log.isDebugEnabled()) {
         log.debug("Invoke method called on " + name);
      }

      MBeanOperationInfo opInfo = null;
      for (MBeanOperationInfo op : opInfos) {
         if (op.getName().equals(name)) {
            opInfo = op;
            break;
         }
      }

      if (opInfo == null) {
         final String msg = "Operation " + name + " not in ModelMBeanInfo";
         throw new MBeanException(new ServiceNotFoundException(msg), msg);
      }

      try {
         Class<?>[] classes = new Class[sig.length];
         for (int i = 0; i < classes.length; i++) {
            classes[i] = getClassForName(sig[i]);
         }
         Method method = getObject().getClass().getMethod(name, classes);
         return method.invoke(getObject(), args);
      } catch (Exception e) {
         throw new MBeanException(e);
      }
   }

   public static Class<?> getClassForName(String name) throws ClassNotFoundException {
      try {
         return (Class<?>) Util.loadClassStrict(name);
      } catch (ClassNotFoundException cnfe) {
         // Could be a primitive - let's check
         for (Class<?> primitive : primitives) if (name.equals(primitive.getName())) return primitive;
      }
      throw new ClassNotFoundException("Class " + name + " cannot be found");
   }

   private void findMethods() {
      // find all methods but don't include methods from Object class
      List<Method> methods = new ArrayList<Method>(Arrays.asList(getObject().getClass()
              .getMethods()));
      List<Method> objectMethods = new ArrayList<Method>(Arrays.asList(Object.class.getMethods()));
      methods.removeAll(objectMethods);

      for (Method method : methods) {
         // does method have @ManagedAttribute annotation?
         ManagedAttribute attr = method.getAnnotation(ManagedAttribute.class);
         if (attr != null) {
            String methodName = method.getName();
            if (!methodName.startsWith("get") && !methodName.startsWith("set")
                    && !methodName.startsWith("is")) {
               if (log.isWarnEnabled())
                  log.warn("method name " + methodName
                          + " doesn't start with \"get\", \"set\", or \"is\""
                          + ", but is annotated with @ManagedAttribute: will be ignored");
            } else {
               MBeanAttributeInfo info = null;
               String attributeName = null;
               boolean writeAttribute = false;
               if (isSetMethod(method)) { // setter
                  attributeName = methodName.substring(3);
                  info = new MBeanAttributeInfo(attributeName, method.getParameterTypes()[0]
                          .getCanonicalName(), attr.description(), true, true, false);
                  writeAttribute = true;
               } else { // getter
                  if (method.getParameterTypes().length == 0
                          && method.getReturnType() != java.lang.Void.TYPE) {
                     boolean hasSetter = atts.containsKey(attributeName);
                     // we found is method
                     if (methodName.startsWith("is")) {
                        attributeName = methodName.substring(2);
                        info = new MBeanAttributeInfo(attributeName, method.getReturnType()
                                .getCanonicalName(), attr.description(), true, hasSetter, true);
                     } else {
                        // this has to be get
                        attributeName = methodName.substring(3);
                        info = new MBeanAttributeInfo(attributeName, method.getReturnType()
                                .getCanonicalName(), attr.description(), true, hasSetter, false);
                     }
                  } else {
                     if (log.isWarnEnabled()) {
                        log.warn("Method " + method.getName()
                                + " must have a valid return type and zero parameters");
                     }
                     continue;
                  }
               }

               AttributeEntry ae = atts.get(attributeName);
               // is it a read method?
               if (!writeAttribute) {
                  // we already have annotated field as read
                  if (ae instanceof FieldAttributeEntry && ae.getInfo().isReadable()) {
                     log.warn("not adding annotated method " + method
                             + " since we already have read attribute");
                  }
                  // we already have annotated set method
                  else if (ae instanceof MethodAttributeEntry) {
                     MethodAttributeEntry mae = (MethodAttributeEntry) ae;
                     if (mae.hasSetMethod()) {
                        atts.put(attributeName, new MethodAttributeEntry(mae.getInfo(), mae
                                .getSetMethod(), method));
                     }
                  } // we don't have such entry
                  else {
                     atts.put(attributeName, new MethodAttributeEntry(info, null, method));
                  }
               }// is it a set method?
               else {
                  if (ae instanceof FieldAttributeEntry) {
                     // we already have annotated field as write
                     if (ae.getInfo().isWritable()) {
                        log.warn("Not adding annotated method " + methodName
                                + " since we already have writable attribute");
                     } else {
                        // we already have annotated field as read
                        // lets make the field writable
                        Field f = ((FieldAttributeEntry) ae).getField();
                        MBeanAttributeInfo i = new MBeanAttributeInfo(ae.getInfo().getName(), f
                                .getType().getCanonicalName(), attr.description(), true, Modifier
                                .isFinal(f.getModifiers()) ? false : true, false);
                        atts.put(attributeName, new FieldAttributeEntry(i, f));
                     }
                  }
                  // we already have annotated getOrIs method
                  else if (ae instanceof MethodAttributeEntry) {
                     MethodAttributeEntry mae = (MethodAttributeEntry) ae;
                     if (mae.hasIsOrGetMethod()) {
                        atts.put(attributeName, new MethodAttributeEntry(info, method, mae
                                .getIsOrGetMethod()));
                     }
                  } // we don't have such entry
                  else {
                     atts.put(attributeName, new MethodAttributeEntry(info, method, null));
                  }
               }
            }
         } else if (method.isAnnotationPresent(ManagedOperation.class)) {
            ManagedOperation op = method.getAnnotation(ManagedOperation.class);
            String attName = method.getName();
            if (isSetMethod(method) || isGetMethod(method)) {
               attName = attName.substring(3);
            } else if (isIsMethod(method)) {
               attName = attName.substring(2);
            }
            // expose unless we already exposed matching attribute field
            boolean isAlreadyExposed = atts.containsKey(attName);
            if (!isAlreadyExposed) {
               ops.add(new MBeanOperationInfo(op != null ? op.description() : "", method));
            }
         }
      }
   }

   private boolean isSetMethod(Method method) {
      return (method.getName().startsWith("set") && method.getParameterTypes().length == 1 && method
              .getReturnType() == java.lang.Void.TYPE);
   }

   private boolean isGetMethod(Method method) {
      return (method.getParameterTypes().length == 0
              && method.getReturnType() != java.lang.Void.TYPE && method.getName().startsWith(
              "get"));
   }

   private boolean isIsMethod(Method method) {
      return (method.getParameterTypes().length == 0
              && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class) && method
              .getName().startsWith("is"));
   }

   private void findFields() {
      // traverse class hierarchy and find all annotated fields
      for (Class<?> clazz = getObject().getClass(); clazz != null; clazz = clazz.getSuperclass()) {

         Field[] fields = clazz.getDeclaredFields();
         for (Field field : fields) {
            ManagedAttribute attr = field.getAnnotation(ManagedAttribute.class);
            if (attr != null) {
               String fieldName = renameToJavaCodingConvention(field.getName());
               MBeanAttributeInfo info = new MBeanAttributeInfo(fieldName, field.getType()
                       .getCanonicalName(), attr.description(), true, !Modifier.isFinal(field
                       .getModifiers()) && attr.writable(), false);

               atts.put(fieldName, new FieldAttributeEntry(info, field));
            }
         }
      }
   }

   private Attribute getNamedAttribute(String name) {
      Attribute result = null;
      if (name.equals(MBEAN_DESCRITION)) {
         result = new Attribute(MBEAN_DESCRITION, this.description);
      } else {
         AttributeEntry entry = atts.get(name);
         if (entry != null) {
            MBeanAttributeInfo i = entry.getInfo();
            try {
               result = new Attribute(name, entry.invoke(null));
               if (log.isDebugEnabled())
                  log
                          .debug("Attribute " + name + " has r=" + i.isReadable() + ",w="
                                  + i.isWritable() + ",is=" + i.isIs() + " and value "
                                  + result.getValue());
            } catch (Exception e) {
               log.debug("Exception while reading value of attribute " + name, e);
            }
         } else {
            log.warn("Did not find queried attribute with name " + name);
         }
      }
      return result;
   }

   private boolean setNamedAttribute(Attribute attribute) {
      boolean result = false;
      if (log.isDebugEnabled())
         log.debug("Invoking set on attribute " + attribute.getName() + " with value "
                 + attribute.getValue());

      AttributeEntry entry = atts.get(attribute.getName());
      if (entry != null) {
         try {
            entry.invoke(attribute);
            result = true;
         } catch (Exception e) {
            log.warn("Exception while writing value for attribute " + attribute.getName(), e);
         }
      } else {
         log.warn("Could not invoke set on attribute " + attribute.getName() + " with value "
                 + attribute.getValue());
      }
      return result;
   }

   private String renameToJavaCodingConvention(String fieldName) {
      if (fieldName.contains("_")) {
         Pattern p = Pattern.compile("_.");
         Matcher m = p.matcher(fieldName);
         StringBuffer sb = new StringBuffer();
         while (m.find()) {
            m.appendReplacement(sb, fieldName.substring(m.end() - 1, m.end()).toUpperCase());
         }
         m.appendTail(sb);
         char first = sb.charAt(0);
         if (Character.isLowerCase(first)) {
            sb.setCharAt(0, Character.toUpperCase(first));
         }
         return sb.toString();
      } else {
         if (Character.isLowerCase(fieldName.charAt(0))) {
            return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
         } else {
            return fieldName;
         }
      }
   }

   private class MethodAttributeEntry implements AttributeEntry {

      final MBeanAttributeInfo info;

      final Method isOrGetmethod;

      final Method setMethod;

      public MethodAttributeEntry(final MBeanAttributeInfo info, final Method setMethod,
                                  final Method isOrGetMethod) {
         super();
         this.info = info;
         this.setMethod = setMethod;
         this.isOrGetmethod = isOrGetMethod;
      }

      public Object invoke(Attribute a) throws Exception {
         if (a == null && isOrGetmethod != null)
            return isOrGetmethod.invoke(getObject());
         else if (a != null && setMethod != null)
            return setMethod.invoke(getObject(), a.getValue());
         else
            return null;
      }

      public MBeanAttributeInfo getInfo() {
         return info;
      }

      public boolean hasIsOrGetMethod() {
         return isOrGetmethod != null;
      }

      public boolean hasSetMethod() {
         return setMethod != null;
      }

      public Method getIsOrGetMethod() {
         return isOrGetmethod;
      }

      public Method getSetMethod() {
         return setMethod;
      }
   }

   private class FieldAttributeEntry implements AttributeEntry {

      private final MBeanAttributeInfo info;

      private final Field field;

      public FieldAttributeEntry(final MBeanAttributeInfo info, final Field field) {
         super();
         this.info = info;
         this.field = field;
         if (!field.isAccessible()) {
            field.setAccessible(true);
         }
      }

      public Field getField() {
         return field;
      }

      public Object invoke(Attribute a) throws Exception {
         if (a == null) {
            return field.get(getObject());
         } else {
            field.set(getObject(), a.getValue());
            return null;
         }
      }

      public MBeanAttributeInfo getInfo() {
         return info;
      }
   }

   private interface AttributeEntry {
      public Object invoke(Attribute a) throws Exception;

      public MBeanAttributeInfo getInfo();
   }

   public boolean isManagedResource() {
      return !atts.isEmpty() || !ops.isEmpty();
   }

   public String getObjectName() {
      MBean mBean = obj.getClass().getAnnotation(MBean.class);
      if (mBean != null && mBean.objectName() != null && mBean.objectName().trim().length() > 0) {
         return mBean.objectName();
      }
      return obj.getClass().getSimpleName();
   }

   public boolean isOperationRegistred(String operationName) {
      for (MBeanOperationInfo opInfo : this.ops) {
         if (opInfo.getName().equals(operationName)) {
            return true;
         }
      }
      return false;
   }
}
