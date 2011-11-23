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
package org.infinispan.jmx;

import static org.infinispan.util.ReflectionUtil.getAnnotation;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import org.infinispan.commons.util.Util;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class was entirely copied from JGroups 2.7 (same name there). Couldn't simply reuse it because JGroups does not
 * ship with MBean, ManagedAttribute and ManagedOperation. Once JGroups will ship these classes, the code can be
 * dynamically reused from there.
 * <p/>
 * The original JGroup's ResourceDMBean logic has been modified so that {@link #invoke()} method checks whether the
 * operation called has been exposed as a {@link ManagedOperation}, otherwise the call fails. JGroups deviated from this
 * logic on purpose because they liked the fact that you could expose all class methods by simply annotating class with
 * {@link MBean} annotation.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ResourceDMBean implements DynamicMBean {
   private static final Class<?>[] primitives = {int.class, byte.class, short.class, long.class,
                                                 float.class, double.class, boolean.class, char.class};

   private static final Class<?>[] primitiveArrays = {int[].class, byte[].class, short[].class, long[].class,
                                                      float[].class, double[].class, boolean[].class, char[].class};

   private static final String MBEAN_DESCRITION = "Dynamic MBean Description";

   private static final Log log = LogFactory.getLog(ResourceDMBean.class);
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

      // TODO: These painful lookups really should be indexed by Jandex at compile-time.  See ISPN-1522
      findDescription();
      findFields();
      findMethods();

      attrInfo = new MBeanAttributeInfo[atts.size()];
      int i = 0;

      MBeanAttributeInfo info;
      for (AttributeEntry entry : atts.values()) {
         info = entry.getInfo();
         attrInfo[i++] = info;
         if (log.isTraceEnabled()) {
            log.tracef("Attribute %s [r=%b,w=%b,is=%b,type=%s]", info.getName(),
                       info.isReadable(), info.isWritable(), info.isIs(), info.getType());
         }
      }

      opInfos = new MBeanOperationInfo[ops.size()];
      ops.toArray(opInfos);

      if (log.isTraceEnabled()) {
         if (!ops.isEmpty())
            log.trace("Operations are:");
         for (MBeanOperationInfo op : opInfos) {
            log.tracef("Operation %s %s", op.getReturnType(), op.getName());
         }
      }
   }

   Object getObject() {
      return obj;
   }

   private synchronized void findDescription() {
      MBean mbean = getAnnotation(getObject().getClass(), MBean.class);
      if (mbean != null && mbean.description() != null && mbean.description().trim().length() > 0) {
         description = mbean.description();
         if (log.isDebugEnabled()) {
            log.debugf("@MBean description set - %s", mbean.description());
         }
         MBeanAttributeInfo info = new MBeanAttributeInfo(MBEAN_DESCRITION, "java.lang.String",
                                                          "@MBean description", true, false, false);
         try {
            atts.put(MBEAN_DESCRITION, new FieldAttributeEntry(info, getClass().getDeclaredField(
                  "description")));
         } catch (NoSuchFieldException e) {
            // this should not happen unless somebody removes description field
            log.couldNotFindDescriptionField();
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
            log.couldNotFindAttribute(name);
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
            log.failedToUpdateAtribute(attr.getName(), attr.getValue());
         }
      }
      return results;
   }

   public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
                                                                         ReflectionException {
      if (log.isDebugEnabled()) {
         log.debugf("Invoke method called on %s", name);
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
            classes[i] = getClassForName(sig[i], null);
         }
         Method method = getObject().getClass().getMethod(name, classes);
         return method.invoke(getObject(), args);
      } catch (Exception e) {
         throw new MBeanException(e);
      }
   }

   public static Class<?> getClassForName(String name, ClassLoader cl) throws ClassNotFoundException {
      try {
         return Util.loadClassStrict(name, cl);
      } catch (ClassNotFoundException cnfe) {
         // Could be a primitive - let's check
         for (Class<?> primitive : primitives) if (name.equals(primitive.getName())) return primitive;
         for (Class<?> primitive : primitiveArrays) if (name.equals(primitive.getName())) return primitive;
      }
      throw new ClassNotFoundException("Class " + name + " cannot be found");
   }

   private void findMethods() {

      for (Method method : ReflectionUtil.getAllMethods(getObject().getClass(), ManagedAttribute.class)) {
         ManagedAttribute attr = method.getAnnotation(ManagedAttribute.class);
         String methodName = method.getName();
         if (!methodName.startsWith("get") && !methodName.startsWith("set")
               && !methodName.startsWith("is")) {
            log.ignoringManagedAttribute(methodName);
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
                  // we found is method
                  if (methodName.startsWith("is")) {
                     attributeName = methodName.substring(2);
                     info = new MBeanAttributeInfo(attributeName, method.getReturnType().getCanonicalName(),
                                                   attr.description(), true, atts.containsKey(attributeName), true);
                  } else {
                     // this has to be get
                     attributeName = methodName.substring(3);
                     info = new MBeanAttributeInfo(attributeName, method.getReturnType().getCanonicalName(),
                                                   attr.description(), true, atts.containsKey(attributeName), false);
                  }
               } else {
                  log.invalidManagedAttributeMethod(method.getName());
                  continue;
               }
            }

            AttributeEntry ae = atts.get(attributeName);
            // is it a read method?
            if (!writeAttribute) {
               // we already have annotated field as read
               if (ae instanceof FieldAttributeEntry && ae.getInfo().isReadable()) {
                  log.readManagedAttributeAlreadyPresent(method);
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
                     log.writeManagedAttributeAlreadyPresent(methodName);
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
      }
      for (Method method : ReflectionUtil.getAllMethods(getObject().getClass(), ManagedOperation.class)) {
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
      for (Field field : ReflectionUtil.getAnnotatedFields(getObject().getClass(), ManagedAttribute.class)) {
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

   private synchronized Attribute getNamedAttribute(String name) {
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
                  log.debugf("Attribute %s has r=%b,w=%b,is=%b and value %s",
                             name, i.isReadable(), i.isWritable(), i.isIs(), result.getValue());
            } catch (Exception e) {
               log.debugf("Exception while reading value of attribute %s: %s", name, e);
            }
         } else {
            log.queriedAttributeNotFound(name);
         }
      }
      return result;
   }

   private boolean setNamedAttribute(Attribute attribute) {
      boolean result = false;
      if (log.isDebugEnabled())
         log.debugf("Invoking set on attribute %s with value %s",
                    attribute.getName(), attribute.getValue());

      AttributeEntry entry = atts.get(attribute.getName());
      if (entry != null) {
         try {
            entry.invoke(attribute);
            result = true;
         } catch (Exception e) {
            log.errorWritingValueForAttribute(attribute.getName(), e);
         }
      } else {
         log.couldNotInvokeSetOnAttribute(attribute.getName(), attribute.getValue());
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
