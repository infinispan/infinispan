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

import org.infinispan.factories.components.JmxAttributeMetadata;
import org.infinispan.factories.components.JmxOperationMetadata;
import org.infinispan.factories.components.JmxOperationParameter;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
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
import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;
import javax.management.ServiceNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.util.ReflectionUtil.EMPTY_CLASS_ARRAY;

/**
 * This class was entirely copied from JGroups 2.7 (same name there). Couldn't simply reuse it because JGroups does not
 * ship with MBean, ManagedAttribute and ManagedOperation. Once JGroups will ship these classes, the code can be
 * dynamically reused from there.
 * <p/>
 * The original JGroup's ResourceDMBean logic has been modified so that invoke() method checks whether the operation
 * called has been exposed as a {@link ManagedOperation}, otherwise the call fails. JGroups deviated from this logic on
 * purpose because they liked the fact that you could expose all class methods by simply annotating class with {@link
 * MBean} annotation.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ResourceDMBean implements DynamicMBean {

   private static final String MBEAN_DESCRITION = "Dynamic MBean Description";

   private static final Log log = LogFactory.getLog(ResourceDMBean.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Object obj;
   private final Class<?> objectClass;
   private final IspnMBeanOperationInfo[] opInfos;
   private final MBeanAttributeInfo[] attInfos;
   private final HashMap<String, InvokableMBeanAttributeInfo> atts = new HashMap<String, InvokableMBeanAttributeInfo>(2);
   private final ManageableComponentMetadata mBeanMetadata;

   private static final Map<String, Field> FIELD_CACHE = ConcurrentMapFactory.makeConcurrentMap(64);
   private static final Map<String, Method> METHOD_CACHE = ConcurrentMapFactory.makeConcurrentMap(64);
   private static final Map<String[], Class<?>[]> PARAM_TYPE_CACHE = ConcurrentMapFactory.makeConcurrentMap(64);

   public ResourceDMBean(Object instance, ManageableComponentMetadata mBeanMetadata) throws NoSuchFieldException, ClassNotFoundException {

      if (instance == null)
         throw new NullPointerException("Cannot make an MBean wrapper for null instance");

      this.obj = instance;
      this.objectClass = instance.getClass();
      this.mBeanMetadata = mBeanMetadata;

      // Load up all fields.
      int i = 0;
      attInfos = new MBeanAttributeInfo[mBeanMetadata.getAttributeMetadata().size()];
      for (JmxAttributeMetadata attributeMetadata : mBeanMetadata.getAttributeMetadata()) {
         String attributeName = attributeMetadata.getName();
         InvokableMBeanAttributeInfo info = toJmxInfo(attributeMetadata);
         if (atts.containsKey(attributeName)) {
            throw new IllegalArgumentException("Component " + mBeanMetadata.getName()
                  + " metadata has a duplicate attribute: " + attributeName);
         }
         atts.put(attributeName, info);
         attInfos[i++] = info.getMBeanAttributeInfo();
         if (trace)
            log.tracef("Attribute %s [r=%b,w=%b,is=%b,type=%s]", attributeName,
                       info.getMBeanAttributeInfo().isReadable(), info.getMBeanAttributeInfo().isWritable(),
                       info.getMBeanAttributeInfo().isIs(), info.getMBeanAttributeInfo().getType());
      }

      // And operations
      IspnMBeanOperationInfo op;
      opInfos = new IspnMBeanOperationInfo[mBeanMetadata.getOperationMetadata().size()];
      i = 0;
      for (JmxOperationMetadata operation : mBeanMetadata.getOperationMetadata()) {
         op = toJmxInfo(operation);
         opInfos[i++] = op;
         if (trace) log.tracef("Operation %s %s", op.getReturnType(), op.getName());
      }
   }

   private static Field findField(Class<?> objectClass, String fieldName) throws NoSuchFieldException {
      String key = objectClass.getName() + "#" + fieldName;
      Field f = FIELD_CACHE.get(key);
      if (f == null) {
         f = ReflectionUtil.getField(fieldName, objectClass);
         if (f != null) FIELD_CACHE.put(key, f);
      }
      return f;
   }

   private static Method findSetter(Class<?> objectClass, String fieldName) throws NoSuchFieldException {
      String key = objectClass.getName() + "#s#" + fieldName;
      Method m = METHOD_CACHE.get(key);
      if (m == null) {
         m = ReflectionUtil.findSetterForField(objectClass, fieldName);
         if (m != null) METHOD_CACHE.put(key, m);
      }
      return m;
   }

   private static Method findGetter(Class<?> objectClass, String fieldName) throws NoSuchFieldException {
      String key = objectClass.getName() + "#g#" + fieldName;
      Method m = METHOD_CACHE.get(key);
      if (m == null) {
         m = ReflectionUtil.findGetterForField(objectClass, fieldName);
         if (m != null) METHOD_CACHE.put(key, m);
      }
      return m;
   }

   private static Class<?>[] getParameterArray(String[] types) throws ClassNotFoundException {
      if (types == null) return null;
      if (types.length == 0) return EMPTY_CLASS_ARRAY;
      Class<?>[] params = PARAM_TYPE_CACHE.get(types);
      if (params == null) {
         params = ReflectionUtil.toClassArray(types);
         if (params == null) params = EMPTY_CLASS_ARRAY;
         PARAM_TYPE_CACHE.put(types, params);
      }
      return params;
   }

   private InvokableMBeanAttributeInfo toJmxInfo(JmxAttributeMetadata attributeMetadata) throws NoSuchFieldException {
      if (!attributeMetadata.isUseSetter()) {
         Field field = findField(objectClass, attributeMetadata.getName());
         if (field != null) {
            return new InvokableFieldBasedMBeanAttributeInfo(attributeMetadata.getName(), attributeMetadata.getType(),
                                                             attributeMetadata.getDescription(), true, attributeMetadata.isWritable(),
                                                             attributeMetadata.isIs(), field, this);
         }
      }

      Method setter = attributeMetadata.isWritable() ? findSetter(objectClass, attributeMetadata.getName()) : null;
      Method getter = findGetter(objectClass, attributeMetadata.getName());
      return new InvokableSetterBasedMBeanAttributeInfo(attributeMetadata.getName(), attributeMetadata.getType(),
                                                        attributeMetadata.getDescription(), true, attributeMetadata.isWritable(),
                                                        attributeMetadata.isIs(), getter, setter, this);
   }

   private IspnMBeanOperationInfo toJmxInfo(JmxOperationMetadata operationMetadata) throws ClassNotFoundException {
      JmxOperationParameter[] parameters = operationMetadata.getMethodParameters();
      MBeanParameterInfo[] params = new MBeanParameterInfo[parameters.length];
      for(int i=0; i< parameters.length; i++) {
         params[i] = new MBeanParameterInfo(parameters[i].getName(), parameters[i].getType(), parameters[i].getDescription());
      }
      return new IspnMBeanOperationInfo(operationMetadata.getMethodName(), operationMetadata.getDescription(), params, operationMetadata.getReturnType(), MBeanOperationInfo.UNKNOWN,
                                        operationMetadata.getOperationName());
   }

   Object getObject() {
      return obj;
   }

   @Override
   public synchronized MBeanInfo getMBeanInfo() {
      //the client doesn't know about IspnMBeanOperationInfo so we need to convert first
      MBeanOperationInfo[] operationInfoForClient = new MBeanOperationInfo[opInfos.length];
      for (int i = 0; i < opInfos.length; i++) {
         IspnMBeanOperationInfo current = opInfos[i];
         operationInfoForClient[i] = new MBeanOperationInfo(current.getOperationName(), current.getDescription(),
                                                            current.getSignature(), current.getReturnType(), MBeanOperationInfo.UNKNOWN);
      }
      return new MBeanInfo(getObject().getClass().getCanonicalName(), mBeanMetadata.getDescription(), attInfos, null, operationInfoForClient, null);
   }

   @Override
   public Object getAttribute(String name) throws AttributeNotFoundException {
      if (name == null || name.length() == 0)
         throw new NullPointerException("Invalid attribute requested " + name);

      Attribute attr = getNamedAttribute(name);
      if (attr == null) {
         throw new AttributeNotFoundException("Unknown attribute '" + name
                                                    + "'. Known attributes names are: " + atts.keySet());
      }
      return attr.getValue();
   }

   @Override
   public synchronized void setAttribute(Attribute attribute) throws AttributeNotFoundException, MBeanException {
      if (attribute == null || attribute.getName() == null)
         throw new NullPointerException("Invalid attribute requested " + attribute);

      setNamedAttribute(attribute);
   }

   @Override
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

   @Override
   public synchronized AttributeList setAttributes(AttributeList list) {
      AttributeList results = new AttributeList();
      for (Object aList : list) {
         Attribute attr = (Attribute) aList;

         try {
            setNamedAttribute(attr);
            results.add(attr);
         } catch (Exception e) {
            log.failedToUpdateAttribute(attr.getName(), attr.getValue());
         }
      }
      return results;
   }

   @Override
   public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
                                                                         ReflectionException {
      if (log.isDebugEnabled()) {
         log.debugf("Invoke method called on %s", name);
      }

      MBeanOperationInfo opInfo = null;
      for (IspnMBeanOperationInfo op : opInfos) {
         if (op.getOperationName().equals(name)) {
            opInfo = op;
            break;
         }
      }

      if (opInfo == null) {
         final String msg = "Operation " + name + " not amongst operations in " + opInfos;
         throw new MBeanException(new ServiceNotFoundException(msg), msg);
      }

      try {
         Class<?>[] classes = new Class[sig.length];
         for (int i = 0; i < classes.length; i++) {
            classes[i] = ReflectionUtil.getClassForName(sig[i], null);
         }
         Method method = getObject().getClass().getMethod(opInfo.getName(), classes);
         return method.invoke(getObject(), args);
      } catch (Exception e) {
         throw new MBeanException(e);
      }
   }

   private Attribute getNamedAttribute(String name) {
      Attribute result = null;
      if (name.equals(MBEAN_DESCRITION)) {
         result = new Attribute(MBEAN_DESCRITION, mBeanMetadata.getDescription());
      } else {
         InvokableMBeanAttributeInfo i = atts.get(name);
         if (i == null && name.length() > 0) {
            // This is legacy.  Earlier versions used an upper-case starting letter for *some* attributes.
            Character firstChar = name.charAt(0);
            if (Character.isUpperCase(firstChar)) {
               name = name.replaceFirst(Character.toString(firstChar), Character.toString(Character.toLowerCase(firstChar)));
               i = atts.get(name);
            }
         }
         if (i != null) {
            try {
               result = new Attribute(name, i.invoke(null));
               if (trace)
                  log.tracef("Attribute %s has r=%b,w=%b,is=%b and value %s",
                        name, i.getMBeanAttributeInfo().isReadable(), i.getMBeanAttributeInfo().isWritable(), i.getMBeanAttributeInfo().isIs(), result.getValue());
            } catch (Exception e) {
               log.debugf("Exception while reading value of attribute %s: %s", name, e);
            }
         } else {
            log.queriedAttributeNotFound(name);
         }
      }
      return result;
   }

   private void setNamedAttribute(Attribute attribute) throws MBeanException, AttributeNotFoundException {
      if (log.isDebugEnabled())
         log.debugf("Invoking set on attribute %s with value %s",
                    attribute.getName(), attribute.getValue());

      String name = attribute.getName();
      InvokableMBeanAttributeInfo i = atts.get(name);
      if (i == null && name.length() > 0) {
         // This is legacy.  Earlier versions used an upper-case starting letter for *some* attributes.
         Character firstChar = name.charAt(0);
         if (Character.isUpperCase(firstChar)) {
            name = name.replaceFirst(Character.toString(firstChar), Character.toString(Character.toLowerCase(firstChar)));
            i = atts.get(name);
         }
      }

      if (i != null) {
         try {
            i.invoke(attribute);
         } catch (Exception e) {
            log.errorWritingValueForAttribute(name, e);
            throw new MBeanException(e, "Error invoking setter for attribute " + name);
         }
      } else {
         log.couldNotInvokeSetOnAttribute(name, attribute.getValue());
         throw new AttributeNotFoundException("Could not find attribute " + name);
      }
   }

   private static abstract class InvokableMBeanAttributeInfo {

      private final MBeanAttributeInfo attributeInfo;

      public InvokableMBeanAttributeInfo(String name, String type, String description, boolean isReadable, boolean isWritable, boolean isIs) {
         attributeInfo = new MBeanAttributeInfo(name, type, description, isReadable, isWritable, isIs);
      }

      public abstract Object invoke(Attribute a) throws IllegalAccessException, InvocationTargetException;

      public MBeanAttributeInfo getMBeanAttributeInfo() {
         return attributeInfo;
      }
   }

   private static class InvokableFieldBasedMBeanAttributeInfo extends InvokableMBeanAttributeInfo {
      private transient final Field field;
      private transient final ResourceDMBean resource;

      public InvokableFieldBasedMBeanAttributeInfo(String name, String type, String description, boolean isReadable, boolean isWritable, boolean isIs, Field field, ResourceDMBean resource) {
         super(name, type, description, isReadable, isWritable, isIs);
         this.field = field;
         this.resource = resource;
      }

      @Override
      public Object invoke(Attribute a) throws IllegalAccessException {
         if (!Modifier.isPublic(field.getModifiers())) field.setAccessible(true);
         if (a == null) {
            return field.get(resource.getObject());
         } else {
            field.set(resource.getObject(), a.getValue());
            return null;
         }
      }
   }

   private static class InvokableSetterBasedMBeanAttributeInfo extends InvokableMBeanAttributeInfo {
      private transient final Method setter;
      private transient final Method getter;
      private transient final ResourceDMBean resource;

      public InvokableSetterBasedMBeanAttributeInfo(String name, String type, String description, boolean isReadable, boolean isWritable, boolean isIs, Method getter, Method setter, ResourceDMBean resource) {
         super(name, type, description, isReadable, isWritable, isIs);
         this.setter = setter;
         this.getter = getter;
         this.resource = resource;
      }

      @Override
      public Object invoke(Attribute a) throws IllegalAccessException, InvocationTargetException {
         if (a == null) {
            if (!Modifier.isPublic(getter.getModifiers())) getter.setAccessible(true);
            return getter.invoke(resource.getObject(), null);
         } else {
            if (!Modifier.isPublic(setter.getModifiers())) setter.setAccessible(true);
            return setter.invoke(resource.getObject(), a.getValue());
         }
      }
   }

   public String getObjectName() {
      String s = mBeanMetadata.getJmxObjectName();
      return (s != null && s.trim().length() > 0) ? s : objectClass.getSimpleName();
   }
}
