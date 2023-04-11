package org.infinispan.server.core.dataconversion.deserializer;

import static java.io.ObjectStreamConstants.SC_BLOCK_DATA;
import static java.io.ObjectStreamConstants.SC_EXTERNALIZABLE;
import static java.io.ObjectStreamConstants.SC_SERIALIZABLE;
import static java.io.ObjectStreamConstants.SC_WRITE_METHOD;
import static java.io.ObjectStreamConstants.STREAM_MAGIC;
import static java.io.ObjectStreamConstants.STREAM_VERSION;
import static java.io.ObjectStreamConstants.TC_ARRAY;
import static java.io.ObjectStreamConstants.TC_BLOCKDATA;
import static java.io.ObjectStreamConstants.TC_BLOCKDATALONG;
import static java.io.ObjectStreamConstants.TC_CLASS;
import static java.io.ObjectStreamConstants.TC_CLASSDESC;
import static java.io.ObjectStreamConstants.TC_ENDBLOCKDATA;
import static java.io.ObjectStreamConstants.TC_ENUM;
import static java.io.ObjectStreamConstants.TC_EXCEPTION;
import static java.io.ObjectStreamConstants.TC_LONGSTRING;
import static java.io.ObjectStreamConstants.TC_NULL;
import static java.io.ObjectStreamConstants.TC_OBJECT;
import static java.io.ObjectStreamConstants.TC_PROXYCLASSDESC;
import static java.io.ObjectStreamConstants.TC_REFERENCE;
import static java.io.ObjectStreamConstants.TC_RESET;
import static java.io.ObjectStreamConstants.TC_STRING;
import static java.io.ObjectStreamConstants.baseWireHandle;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.StreamCorruptedException;
import java.io.WriteAbortedException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class Deserializer {

   private static final SEntity END = new SString("END");
   private final PrimitiveClassDescFactory primitiveClassDescFactory = new PrimitiveClassDescFactory();
   private final DataInputStream din;
   private final List<SEntity> handles = new ArrayList<>();
   private final Map<String, ClassDesc> classDescriptions = new HashMap<>();

   public Deserializer(InputStream in, boolean header) throws IOException {
      this.din = new DataInputStream(in);
      if (header) {
         if (din.readShort() != STREAM_MAGIC || din.readShort() != STREAM_VERSION)
            throw new StreamCorruptedException("Bad stream header");
      }
   }

   public SEntity readObject() throws IOException {
      SEntity entity = readObjectOrEnd();
      if (entity == END) throw new StreamCorruptedException("Unexpected end-block-data");
      return entity;
   }

   private SString readString() throws IOException {
      return (SString) readObject();
   }

   private SEntity readObjectOrEnd() throws IOException {
      while (true) {
         int code = din.readByte();
         switch (code) {
            case TC_OBJECT:
               return newObject();
            case TC_CLASS:
               return newClass();
            case TC_ARRAY:
               return newArray();
            case TC_STRING:
               return newString();
            case TC_LONGSTRING:
               return newLongString();
            case TC_ENUM:
               return newEnum();
            case TC_CLASSDESC:
            case TC_PROXYCLASSDESC:
               classDesc(code);
               break;
            case TC_REFERENCE:
               return prevObject();
            case TC_NULL:
               return null;
            case TC_EXCEPTION:
               exception();
               break;
            case TC_RESET:
               reset();
               break;
            case TC_BLOCKDATA:
               return blockDataShort();
            case TC_BLOCKDATALONG:
               return blockDataLong();
            case TC_ENDBLOCKDATA:
               return END;
            default:
               throw new StreamCorruptedException("Bad type code: " + code);
         }
      }
   }

   private SEntity newObject() throws IOException {
      ObjectClassDesc desc = classDesc();
      SObject t = new SObject(desc.getType());
      newHandle(t);
      for (ObjectClassDesc cd : desc.getHierarchy())
         classData(t, cd);
      return t;
   }

   private void classData(SObject t, ObjectClassDesc cd) throws IOException {
      int flags = cd.getFlags();
      if ((flags & SC_SERIALIZABLE) != 0) {
         for (FieldDesc fieldDesc : cd.getFields()) {
            t.setField(fieldDesc.getName(), fieldDesc.read());
         }
         if ((flags & SC_WRITE_METHOD) != 0) {
            objectAnnotation(t);
         }
      } else if ((flags & SC_EXTERNALIZABLE) != 0) {
         if ((flags & SC_BLOCK_DATA) == 0) throw new IOException("Can't handle externalContents");
         objectAnnotation(t);
      }
   }

   private void objectAnnotation(SObject t) throws IOException {
      while (readObjectOrEnd() != END) {
         // Discard annotations
      }
   }

   private ClassDesc newClass() throws IOException {
      ClassDesc desc = classDesc();
      newHandle(desc);
      return desc;
   }

   private ObjectClassDesc classDesc() throws IOException {
      int code = din.readByte();
      return classDesc(code);
   }

   private ObjectClassDesc classDesc(int code) throws IOException {
      return classDesc0(code);
   }

   private ObjectClassDesc classDesc0(int code) throws IOException {
      switch (code) {
         case TC_CLASSDESC:
            return newPlainClassDesc();
         case TC_PROXYCLASSDESC:
            return newProxyClassDesc();
         case TC_NULL:
            return null;
         case TC_REFERENCE:
            return (ObjectClassDesc) prevObject();
         default:
            throw new StreamCorruptedException("Bad class descriptor");
      }
   }

   private ObjectClassDesc newPlainClassDesc() throws IOException {
      String className = din.readUTF();
      long serialVersionUID = din.readLong();
      int flags = din.readByte();
      ObjectClassDesc desc;
      if (className.startsWith("[")) desc = getArrayClassDesc(className, flags, serialVersionUID);
      else desc = getObjectClassDesc(className, flags, serialVersionUID);
      newHandle(desc);
      int nfields = din.readShort();
      FieldDesc[] fields = new FieldDesc[nfields];
      for (int i = 0; i < nfields; i++)
         fields[i] = fieldDesc();
      desc.setFields(fields);
      classAnnotation(desc);
      ObjectClassDesc superDesc = classDesc();
      desc.setSuperClassDesc(superDesc);
      return desc;
   }

   private ObjectClassDesc getObjectClassDesc(String className, int flags, Long serialVersionUID) {
      ObjectClassDesc desc = (ObjectClassDesc) classDescriptions.get(className);
      if (desc != null) {
         if (desc.getSerialVersionUID() == null) desc.setSerialVersionUID(serialVersionUID);
         if (!desc.getName().equals(className) || desc.getFlags() != flags)
            throw new IllegalStateException("name/flags/serialVersionUID don't match");
         return desc;
      } else {
         desc = new ObjectClassDesc(className, flags, serialVersionUID);
         classDescriptions.put(desc.getName(), desc);
         return desc;
      }
   }

   private ArrayClassDesc getArrayClassDesc(String className, int flags, Long serialVersionUID) throws IOException {
      ArrayClassDesc desc = (ArrayClassDesc) classDescriptions.get(className);
      if (desc != null) {
         if (desc.getSerialVersionUID() == null) desc.setSerialVersionUID(serialVersionUID);
         if (!desc.getName().equals(className) || desc.getFlags() != flags)
            throw new IllegalStateException("name/flags/serialVersionUID don't match");
         return desc;
      } else {
         desc = new ArrayClassDesc(className, flags, serialVersionUID);
         classDescriptions.put(desc.getName(), desc);
         return desc;
      }
   }

   private ObjectClassDesc newProxyClassDesc() throws IOException {
      ObjectClassDesc desc = new ObjectClassDesc("<Proxy>", SC_SERIALIZABLE, 1L);
      newHandle(desc);
      // Discard the interfaces
      int count = din.readInt();
      for (int i = 0; i < count; i++) din.readUTF();
      classAnnotation(desc);
      ObjectClassDesc superDesc = classDesc();
      desc.setSuperClassDesc(superDesc);
      return desc;
   }

   private void classAnnotation(ClassDesc desc) throws IOException {
      while (readObjectOrEnd() != END) {
         // Discard the annotations
      }
   }

   private FieldDesc fieldDesc() throws IOException {
      char c = (char) din.readByte();
      final boolean primitive;
      switch (c) {
         case 'B':
         case 'C':
         case 'D':
         case 'F':
         case 'I':
         case 'J':
         case 'S':
         case 'Z':
            primitive = true;
            break;
         case 'L':
         case '[':
            primitive = false;
            break;
         default:
            throw new StreamCorruptedException("Bad field type " + (int) c);
      }
      String name = din.readUTF();
      FieldDesc desc;
      if (primitive) desc = new PrimitiveFieldDesc(name, c);
      else {
         String className = readString().getValue();
         if (className.startsWith("L")) {
            className = className.substring(1, className.length() - 1);
         }
         className = className.replaceAll("/", ".");
         desc = new ReferenceFieldDesc(name, className);
      }
      return desc;
   }

   private SArray newArray() throws IOException {
      ArrayClassDesc classDesc = (ArrayClassDesc) classDesc();
      int size = din.readInt();
      SArray array = new SArray(classDesc.getType(), size);
      newHandle(array);
      ClassDesc componentClassDesc = classDesc.getComponentClassDesc();
      for (int i = 0; i < size; i++)
         array.set(i, componentClassDesc.read());
      return array;
   }

   private SString newString() throws IOException {
      SString s = new SString(din.readUTF());
      newHandle(s);
      return s;
   }

   private SString newLongString() throws IOException {
      long len = din.readLong();
      StringBuilder sb = new StringBuilder();
      while (len > 0) {
         int slice = (int) Math.min(len, 65535);
         byte[] blen = {(byte) (slice >> 8), (byte) slice};
         InputStream lenis = new ByteArrayInputStream(blen);
         InputStream seqis = new SequenceInputStream(lenis, din);
         DataInputStream ddin = new DataInputStream(seqis);
         String s = ddin.readUTF();
         assert s.length() == slice;
         sb.append(s);
         len -= slice;
      }
      return new SString(sb.toString());
   }

   private SObject newEnum() throws IOException {
      ObjectClassDesc classDesc = classDesc();
      SObject enumConst = new SObject(classDesc.getType());
      newHandle(enumConst);
      SString constName = readString();
      classDesc.getEnumValues().add(constName.getValue());
      enumConst.setField("<name>", constName);
      return enumConst;
   }

   private void exception() throws IOException {
      reset();
      IOException exc = new IOException(readObject().toString());
      reset();
      throw new WriteAbortedException("Writing aborted", exc);
   }

   private SBlockData blockDataShort() throws IOException {
      int len = din.readUnsignedByte();
      return blockData(len);
   }

   private SBlockData blockDataLong() throws IOException {
      int len = din.readInt();
      return blockData(len);
   }

   private SBlockData blockData(int len) throws IOException {
      byte[] data = new byte[len];
      din.readFully(data);
      return new SBlockData(data);
   }

   private void newHandle(SEntity o) {
      handles.add(o);
   }

   private SEntity prevObject() throws IOException {
      int h = din.readInt();
      int i = h - baseWireHandle;
      if (i < 0 || i > handles.size()) throw new StreamCorruptedException("Bad handle: " + h);
      return handles.get(i);
   }

   private void reset() {
      handles.clear();
   }

   public abstract static class ClassDesc extends SEntity {
      private final String name;

      ClassDesc(String name) {
         super(name);
         this.name = name;
      }

      @Override
      public Json json() {
         throw new UnsupportedOperationException();
      }

      public abstract String toString();

      abstract SEntity read() throws IOException;

      abstract Class<?> arrayComponentClass();

      public String getName() {
         return name;
      }
   }

   public class ObjectClassDesc extends ClassDesc {
      private final int flags;
      private final List<ObjectClassDesc> hierarchy = new ArrayList<>();
      private final Set<String> enumValues = new HashSet<>();
      private Long serialVersionUID;
      private FieldDesc[] fields = new FieldDesc[0];
      private ObjectClassDesc superClassDesc;

      ObjectClassDesc(String name, int flags, Long serialVersionUID) {
         super(name);
         this.flags = flags;
         this.serialVersionUID = serialVersionUID;
      }

      public Long getSerialVersionUID() {
         return serialVersionUID;
      }

      public void setSerialVersionUID(Long serialVersionUID) {
         this.serialVersionUID = serialVersionUID;
      }

      SEntity read() throws IOException {
         return readObject();
      }

      Class<?> arrayComponentClass() {
         if (getType().equals("java.lang.String")) return String.class;
         else return SEntity.class;
      }

      public String toString() {
         return getType();
      }

      public FieldDesc[] getFields() {
         return fields;
      }

      public void setFields(FieldDesc[] fields) {
         this.fields = fields;
      }

      public int getFlags() {
         return flags;
      }

      public void setSuperClassDesc(ObjectClassDesc superClassDesc) {
         this.superClassDesc = superClassDesc;
      }

      public List<ObjectClassDesc> getHierarchy() {
         if (hierarchy.isEmpty()) {
            if (superClassDesc != null) hierarchy.addAll(superClassDesc.getHierarchy());
            hierarchy.add(this);
         }
         return hierarchy;
      }

      public Set<String> getEnumValues() {
         return enumValues;
      }
   }

   public class ArrayClassDesc extends ObjectClassDesc {
      private final ClassDesc componentClassDesc;
      private final Class<?> arrayClass;

      ArrayClassDesc(String name, int flags, long serialVersionUID) throws IOException {
         super(name, flags, serialVersionUID);
         String componentName = name.substring(1);
         if (componentName.startsWith("[")) componentClassDesc = getArrayClassDesc(componentName, flags, null);
         else if (componentName.startsWith("L")) {
            componentName = componentName.substring(1, componentName.length() - 1);
            componentClassDesc = getObjectClassDesc(componentName, flags, null);
         } else {
            if (componentName.length() > 1) throw new StreamCorruptedException("Bad array type " + name);
            char typeCode = componentName.charAt(0);
            componentClassDesc = primitiveClassDescFactory.forTypeCode(typeCode);
         }
         Class<?> componentClass = componentClassDesc.arrayComponentClass();
         arrayClass = Array.newInstance(componentClass, 0).getClass();
      }

      Class<?> arrayComponentClass() {
         return arrayClass;
      }

      public ClassDesc getComponentClassDesc() {
         return componentClassDesc;
      }
   }

   class PrimitiveClassDescFactory {
      private final Map<Character, PrimitiveClassDesc> DESCRIPTORS = new HashMap<>();

      {
         String primitives = "BByte CChar DDouble FFloat IInt JLong SShort ZBoolean";
         for (String prim : primitives.split(" ")) {
            char typeCode = prim.charAt(0);
            String readWhat = prim.substring(1);
            Method readMethod;
            try {
               readMethod = DataInputStream.class.getMethod("read" + readWhat);
            } catch (Exception e) {
               throw new RuntimeException("No read method for " + readWhat, e);
            }
            Class<?> componentClass;
            try {
               Class<?> arrayClass = Class.forName("[" + typeCode);
               componentClass = arrayClass.getComponentType();
            } catch (Exception e) {
               throw new RuntimeException("No array class for " + typeCode, e);
            }
            PrimitiveClassDesc desc = new PrimitiveClassDesc(typeCode, readMethod, componentClass);
            DESCRIPTORS.put(typeCode, desc);
         }
      }

      PrimitiveClassDesc forTypeCode(char c) throws IOException {
         PrimitiveClassDesc desc = DESCRIPTORS.get(c);
         if (desc == null) throw new StreamCorruptedException("Bad type code " + (int) c);
         return desc;
      }
   }

   public class PrimitiveClassDesc extends ClassDesc {
      private final Method readMethod;
      private final Class<?> componentClass;

      PrimitiveClassDesc(char typeCode, Method readMethod, Class<?> componentClass) {
         super(String.valueOf(typeCode));
         this.readMethod = readMethod;
         this.componentClass = componentClass;
      }

      SEntity read() throws IOException {
         try {
            Object wrapped = readMethod.invoke(din);
            return new SPrim(wrapped);
         } catch (IllegalAccessException e) {
            throw new RuntimeException("Read method invoke failed", e);
         } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t instanceof IOException) throw (IOException) t;
            else if (t instanceof RuntimeException) throw (RuntimeException) t;
            else if (t instanceof Error) throw (Error) t;
            else throw new RuntimeException(t.toString(), t);
         }
      }

      Class<?> arrayComponentClass() {
         return componentClass;
      }

      public String toString() {
         return componentClass.getName();
      }
   }

   public abstract static class FieldDesc {
      private final String name;

      FieldDesc(String name) {
         this.name = name;
      }

      abstract SEntity read() throws IOException;

      public abstract String toString();

      public String getName() {
         return this.name;
      }

      public abstract String getTypeName();
   }

   public class ReferenceFieldDesc extends FieldDesc {
      private final String className;

      ReferenceFieldDesc(String name, String className) {
         super(name);
         this.className = className;
      }

      SEntity read() throws IOException {
         return readObject();
      }

      public String toString() {
         return className + " " + getName();
      }

      @Override
      public String getTypeName() {
         return className;
      }
   }

   public class PrimitiveFieldDesc extends FieldDesc {
      private final PrimitiveClassDesc classDesc;

      PrimitiveFieldDesc(String name, char type) throws IOException {
         super(name);
         classDesc = primitiveClassDescFactory.forTypeCode(type);
      }

      SEntity read() throws IOException {
         return classDesc.read();
      }

      public String toString() {
         return classDesc + " " + getName();
      }

      @Override
      public String getTypeName() {
         return classDesc.toString();
      }
   }
}
