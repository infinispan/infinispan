package org.infinispan.jmx;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ServiceNotFoundException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class was copied from JGroups and adapted.
  * The original JGroup's ResourceDMBean logic has been modified so that invoke() method checks whether the operation
 * called has been exposed as a {@link ManagedOperation}, otherwise the call fails. JGroups deviated from this logic on
 * purpose because they liked the fact that you could expose all class methods by simply annotating class with {@link
 * MBean} annotation.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class ResourceDMBean implements DynamicMBean, MBeanRegistration {

   private static final Log log = LogFactory.getLog(ResourceDMBean.class);

   private final Object obj;
   private final Class<?> objectClass;
   private final MBeanOperationInfo[] opInfos;
   private final String[] opNames;
   private final MBeanAttributeInfo[] attInfos;
   private final Map<String, InvokableMBeanAttributeInfo> atts = new HashMap<>(2);
   private final String mbeanName;
   private final String description;

   /**
    * This is the name under which this MBean was registered.
    */
   private ObjectName objectName;

   private static final Map<String, Field> FIELD_CACHE = new ConcurrentHashMap<>(64);
   private static final Map<String, Method> METHOD_CACHE = new ConcurrentHashMap<>(64);

   ResourceDMBean(Object instance, MBeanMetadata mBeanMetadata, String componentName) {
      if (instance == null) {
         throw new NullPointerException("Cannot make an MBean wrapper for null instance");
      }
      this.obj = instance;
      this.objectClass = instance.getClass();
      if (mBeanMetadata.getJmxObjectName() != null) {
         mbeanName = mBeanMetadata.getJmxObjectName();
      } else if (componentName != null) {
         mbeanName = componentName;
      } else {
         throw new IllegalArgumentException("MBean.objectName and componentName cannot be both null");
      }
      this.description = mBeanMetadata.getDescription();

      // Load up all fields.
      int i = 0;
      attInfos = new MBeanAttributeInfo[mBeanMetadata.getAttributes().size()];
      for (MBeanMetadata.AttributeMetadata attributeMetadata : mBeanMetadata.getAttributes()) {
         String attributeName = attributeMetadata.getName();
         if (atts.containsKey(attributeName)) {
            throw new IllegalArgumentException("Component " + objectClass.getName()
                  + " metadata has a duplicate attribute: " + attributeName);
         }
         InvokableMBeanAttributeInfo info = toJmxInfo(attributeMetadata);
         atts.put(attributeName, info);
         attInfos[i++] = info.attributeInfo;
         if (log.isTraceEnabled())
            log.tracef("Attribute %s [r=%b,w=%b,is=%b,type=%s]", attributeName,
                  info.attributeInfo.isReadable(), info.attributeInfo.isWritable(),
                  info.attributeInfo.isIs(), info.attributeInfo.getType());
      }

      // And operations
      opInfos = new MBeanOperationInfo[mBeanMetadata.getOperations().size()];
      opNames = new String[opInfos.length];
      i = 0;
      for (MBeanMetadata.OperationMetadata operation : mBeanMetadata.getOperations()) {
         opNames[i] = operation.getOperationName();
         MBeanOperationInfo op = toJmxInfo(operation);
         opInfos[i++] = op;
         if (log.isTraceEnabled()) log.tracef("Operation %s %s", op.getReturnType(), op.getName());
      }
   }

   /**
    * The name assigned via {@link MBean#objectName} or generated based on default rules if missing.
    */
   String getMBeanName() {
      return mbeanName;
   }

   /**
    * The ObjectName. Only available if the MBean was registered.
    */
   public ObjectName getObjectName() {
      return objectName;
   }

   private static Field findField(Class<?> objectClass, String fieldName) {
      String key = objectClass.getName() + "#" + fieldName;
      Field f = FIELD_CACHE.get(key);
      if (f == null) {
         f = ReflectionUtil.getField(fieldName, objectClass);
         if (f != null) FIELD_CACHE.put(key, f);
      }
      return f;
   }

   private static Method findSetter(Class<?> objectClass, String fieldName) {
      String key = objectClass.getName() + "#s#" + fieldName;
      Method m = METHOD_CACHE.get(key);
      if (m == null) {
         m = ReflectionUtil.findSetterForField(objectClass, fieldName);
         if (m != null) METHOD_CACHE.put(key, m);
      }
      return m;
   }

   private static Method findGetter(Class<?> objectClass, String fieldName) {
      String key = objectClass.getName() + "#g#" + fieldName;
      Method m = METHOD_CACHE.get(key);
      if (m == null) {
         m = ReflectionUtil.findGetterForField(objectClass, fieldName);
         if (m != null) METHOD_CACHE.put(key, m);
      }
      return m;
   }

   private InvokableMBeanAttributeInfo toJmxInfo(MBeanMetadata.AttributeMetadata attributeMetadata) {
      if (!attributeMetadata.isUseSetter()) {
         Field field = findField(objectClass, attributeMetadata.getName());
         if (field != null) {
            return new InvokableFieldBasedMBeanAttributeInfo(attributeMetadata.getName(), attributeMetadata.getType(),
                                                             attributeMetadata.getDescription(), true, attributeMetadata.isWritable(),
                                                             attributeMetadata.isIs(), field);
         }
      }

      Method setter = null;
      Method getter = null;
      try {
         setter = attributeMetadata.isWritable() ? findSetter(objectClass, attributeMetadata.getName()) : null;
         getter = findGetter(objectClass, attributeMetadata.getName());
      } catch (NoClassDefFoundError ignored) {
         // missing dependency
      }
      assert getter != null : attributeMetadata;
      return new InvokableSetterBasedMBeanAttributeInfo(attributeMetadata.getName(), attributeMetadata.getType(),
                                                        attributeMetadata.getDescription(), true, attributeMetadata.isWritable(),
                                                        attributeMetadata.isIs(), getter, setter);
   }

   private MBeanOperationInfo toJmxInfo(MBeanMetadata.OperationMetadata operationMetadata) {
      MBeanMetadata.OperationParameterMetadata[] parameters = operationMetadata.getMethodParameters();
      MBeanParameterInfo[] params = new MBeanParameterInfo[parameters.length];
      for (int i = 0; i < parameters.length; i++) {
         params[i] = new MBeanParameterInfo(parameters[i].getName(), parameters[i].getType(), parameters[i].getDescription());
      }
      return new MBeanOperationInfo(operationMetadata.getMethodName(), operationMetadata.getDescription(),
            params, operationMetadata.getReturnType(), MBeanOperationInfo.UNKNOWN);
   }

   @Override
   public MBeanInfo getMBeanInfo() {
      return new MBeanInfo(objectClass.getName(), description, attInfos, null, opInfos, null);
   }

   @Override
   public Object getAttribute(String name) throws AttributeNotFoundException {
      if (name == null || name.isEmpty())
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
            //todo [anistor] is it ok to ignore missing attributes ?
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
   public Object invoke(String name, Object[] args, String[] sig) throws MBeanException {
      if (log.isDebugEnabled()) {
         log.debugf("Invoke method called on %s", name);
      }

      MBeanOperationInfo opInfo = null;
      for (int i = 0; i < opNames.length; i++) {
         if (opNames[i].equals(name)) {
            opInfo = opInfos[i];
            break;
         }
      }

      if (opInfo == null) {
         final String msg = "Operation " + name + " not amongst operations in " + Arrays.toString(opInfos);
         throw new MBeanException(new ServiceNotFoundException(msg), msg);
      }

      // Argument type transformation according to signatures
      for (int i = 0; i < sig.length; i++) {
         // Some clients (e.g. RHQ) will pass the arguments as java.lang.String but we need some fields to be numbers
         if (args[i] != null) {
            if (log.isDebugEnabled())
               log.debugf("Argument value before transformation: %s and its class: %s. " +
                     "For method.invoke we need it to be class: %s", args[i], args[i].getClass(), sig[i]);
            if (sig[i].equals(int.class.getName()) || sig[i].equals(Integer.class.getName())) {
               if (args[i].getClass() != Integer.class && args[i].getClass() != int.class)
                  args[i] = Integer.parseInt((String) args[i]);
            } else if (sig[i].equals(Long.class.getName()) || sig[i].equals(long.class.getName())) {
               if (args[i].getClass() != Long.class && args[i].getClass() != long.class)
                  args[i] = Long.parseLong((String) args[i]);
            }
         }
      }

      try {
         Class<?>[] classes = new Class[sig.length];
         for (int i = 0; i < classes.length; i++) {
            classes[i] = ReflectionUtil.getClassForName(sig[i], null);
         }
         Method method = objectClass.getMethod(opInfo.getName(), classes);
         return method.invoke(obj, args);
      } catch (Exception e) {
         throw new MBeanException(new Exception(getRootCause(e)));
      }
   }

   private static Throwable getRootCause(Throwable throwable) {
      Throwable cause;
      while ((cause = throwable.getCause()) != null) {
         throwable = cause;
      }
      return throwable;
   }

   private Attribute getNamedAttribute(String name) {
      Attribute result = null;

      InvokableMBeanAttributeInfo i = atts.get(name);
      if (i == null && !name.isEmpty()) {
         // This is legacy.  Earlier versions used an upper-case starting letter for *some* attributes.
         char firstChar = name.charAt(0);
         if (Character.isUpperCase(firstChar)) {
            name = Character.toLowerCase(firstChar) + name.substring(1);
            i = atts.get(name);
         }
      }

      if (i != null) {
         try {
            result = new Attribute(name, i.invoke(null));
            if (log.isTraceEnabled())
               log.tracef("Attribute %s has r=%b,w=%b,is=%b and value %s",
                     name, i.attributeInfo.isReadable(), i.attributeInfo.isWritable(), i.attributeInfo.isIs(), result.getValue());
         } catch (Exception e) {
            log.debugf(e, "Exception while reading value of attribute %s", name);
            throw new CacheException(e);
         }
      } else {
         log.queriedAttributeNotFound(name);
         //todo [anistor] why not throw an AttributeNotFoundException ?
      }

      return result;
   }

   private void setNamedAttribute(Attribute attribute) throws MBeanException, AttributeNotFoundException {
      if (log.isDebugEnabled()) {
         log.debugf("Invoking set on attribute %s with value %s", attribute.getName(), attribute.getValue());
      }

      String name = attribute.getName();
      InvokableMBeanAttributeInfo i = atts.get(name);
      if (i == null && !name.isEmpty()) {
         // This is legacy.  Earlier versions used an upper-case starting letter for *some* attributes.
         char firstChar = name.charAt(0);
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

   @Override
   public ObjectName preRegister(MBeanServer server, ObjectName name) {
      objectName = name;
      return name;
   }

   @Override
   public void postRegister(Boolean registrationDone) {
   }

   @Override
   public void preDeregister() {
   }

   @Override
   public void postDeregister() {
      objectName = null;
   }

   private abstract static class InvokableMBeanAttributeInfo {

      private final MBeanAttributeInfo attributeInfo;

      InvokableMBeanAttributeInfo(String name, String type, String description, boolean isReadable, boolean isWritable, boolean isIs) {
         attributeInfo = new MBeanAttributeInfo(name, type, description, isReadable, isWritable, isIs);
      }

      abstract Object invoke(Attribute a) throws IllegalAccessException, InvocationTargetException;
   }

   private final class InvokableFieldBasedMBeanAttributeInfo extends InvokableMBeanAttributeInfo {

      private final Field field;

      InvokableFieldBasedMBeanAttributeInfo(String name, String type, String description, boolean isReadable,
                                            boolean isWritable, boolean isIs, Field field) {
         super(name, type, description, isReadable, isWritable, isIs);
         this.field = field;
      }

      @Override
      Object invoke(Attribute a) throws IllegalAccessException {
         if (!Modifier.isPublic(field.getModifiers())) field.setAccessible(true);
         if (a == null) {
            return field.get(obj);
         } else {
            field.set(obj, a.getValue());
            return null;
         }
      }
   }

   private final class InvokableSetterBasedMBeanAttributeInfo extends InvokableMBeanAttributeInfo {
      private final Method setter;
      private final Method getter;

      InvokableSetterBasedMBeanAttributeInfo(String name, String type, String description, boolean isReadable,
                                             boolean isWritable, boolean isIs, Method getter, Method setter) {
         super(name, type, description, isReadable, isWritable, isIs);
         this.setter = setter;
         this.getter = getter;
      }

      @Override
      Object invoke(Attribute a) throws IllegalAccessException, InvocationTargetException {
         if (a == null) {
            if (!Modifier.isPublic(getter.getModifiers())) getter.setAccessible(true);
            return getter.invoke(obj);
         } else {
            if (!Modifier.isPublic(setter.getModifiers())) setter.setAccessible(true);
            return setter.invoke(obj, a.getValue());
         }
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != ResourceDMBean.class) return false;
      ResourceDMBean that = (ResourceDMBean) o;
      return obj == that.obj;  // == is intentional
   }

   @Override
   public int hashCode() {
      return obj.hashCode();
   }

   @Override
   public String toString() {
      return "ResourceDMBean{" +
            "obj=" + System.identityHashCode(obj) +
            ", objectClass=" + objectClass +
            ", mbeanName='" + mbeanName + '\'' +
            ", description='" + description + '\'' +
            ", objectName=" + objectName +
            '}';
   }
}
