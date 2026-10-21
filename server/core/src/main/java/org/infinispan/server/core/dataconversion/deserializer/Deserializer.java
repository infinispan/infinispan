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
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.StreamCorruptedException;
import java.io.WriteAbortedException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.internal.Json;

/**
 * Based on Serialisys by Eamonn McManus
 */
public class Deserializer {

   private static final SEntity END = new SString("END");
   // a misaligned parse can interpret garbage bytes as a length; refuse to allocate on its behalf
   private static final int MAX_ARRAY_SIZE = 1 << 29;
   private static final long MAX_BLOCK_DATA_LENGTH = 1L << 30;
   // granularity at which unrendered block-data payloads are read and discarded
   private static final int BLOCK_DATA_CHUNK = 8192;
   // a writeObject() may write custom data before calling defaultWriteObject(): the declared fields are tried after
   // at most this many leading custom-data chunks, so a section of many chunks cannot drive unbounded re-parsing
   private static final int MAX_CUSTOM_DATA_PREFIX = 8;
   private final PrimitiveClassDescFactory primitiveClassDescFactory = new PrimitiveClassDescFactory();
   private final MarkedStream markedIn;
   private final DataInputStream din;
   private final List<SEntity> handles = new ArrayList<>();
   // the physical index in the handle list where the current logical table starts: a TC_RESET restarts the handle
   // numbering without discarding the entries, so a speculative attempt can be rolled back by truncating the list
   // instead of copying it (see prevObject and UndoLog)
   private int handleBase;
   private final Map<String, ClassDesc> classDescriptions = new HashMap<>();
   // the undo log of the speculative attempts currently in progress, null when none is (see readCustomWriterSlot)
   private UndoLog undoLog;

   public Deserializer(InputStream in, boolean header) throws IOException {
      this.markedIn = new MarkedStream(in);
      this.din = new DataInputStream(markedIn);
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
      if ((flags & SC_SERIALIZABLE) != 0 && (flags & SC_WRITE_METHOD) == 0) {
         readDeclaredFields(t, cd);
      } else if ((flags & SC_SERIALIZABLE) != 0) {
         // A class that declares its own writeObject() produces a section containing the default field values and custom data in either order, or only custom data when defaultWriteObject() was not called. The stream does not record the order, so the section is parsed under several interpretations (see readCustomWriterSlot) and rendered as custom data only when the layout is ambiguous or unparseable as fields
         readCustomWriterSlot(t, cd);
      } else if ((flags & SC_EXTERNALIZABLE) != 0) {
         if ((flags & SC_BLOCK_DATA) == 0) throw new IOException("Can't handle externalContents");
         objectAnnotation(t);
      }
   }

   private void readDeclaredFields(SObject t, ObjectClassDesc cd) throws IOException {
      for (FieldDesc fieldDesc : cd.getFields()) {
         t.setField(fieldDesc.getName(), fieldDesc.read());
      }
   }

   /**
    * Reads the section of object data belonging to a class that declares its own writeObject(). The stream does not
    * record whether custom data was written before or after the default field values, so the section is parsed under
    * several interpretations: the declared fields starting at the section start (custom data after the defaults, the
    * documented layout), or after each of the leading custom-data chunks (custom data before the defaults, which a
    * writer emits as block-data records or nested objects). When the section walks as custom data and opens with a
    * block-data record the fields cannot start at the section start — defaultWriteObject() writes them raw — so that
    * interpretation is deferred to a last resort. Exactly one interpretation must parse to the section's terminator:
    * when several do, the layout is ambiguous and the section is rendered as custom data only rather than presenting
    * speculative field values as correct. Note that custom data written as plain objects before defaultWriteObject()
    * is indistinguishable from reference field values, so such sections keep the fields-first interpretation.
    */
   private void readCustomWriterSlot(SObject t, ObjectClassDesc cd) throws IOException {
      // the object's own field values are snapshotted per section: they are bounded by the class's field count. The
      // accumulated parse state — the handle table and the descriptors — is not snapshotted at all: a stream with many
      // custom-writer sections would copy entries accumulated from all earlier objects on every section, making
      // conversion quadratic. Instead each speculative attempt opens an undo-log savepoint recording only what that
      // attempt changes (see beginAttempt): handles are only appended and are undone by truncation, and a TC_RESET
      // moves the logical base of the handle table (see reset) rather than discarding the entries. Descriptors record
      // their pristine state on their first mutation within an attempt, because a misaligned parse can mutate an
      // existing descriptor — its fields, superclass, cached hierarchy or enum values — when a garbage byte happens to
      // spell a known class name, and leaving that in place would corrupt subsequent objects
      Map<String, SEntity> savedFields = t.fieldSnapshot();
      int mark = markedIn.mark();

      // Walk the section as custom data first. This locates the section's terminator, which rejects interpretations
      // that end inside a block-data payload or run past the section into the following object's bytes, and counts
      // the chunk boundaries at which the declared fields could start. The walk stops where the section stops being
      // custom data: the raw field values, if present, are not chunks.
      Savepoint attempt = beginAttempt();
      long terminator = -1;
      boolean opensWithBlockData = false;
      int chunks = 0;
      while (true) {
         SEntity chunk;
         try {
            chunk = readObjectOrEnd();
         } catch (Exception e) {
            break;
         }
         if (chunk == END) {
            terminator = markedIn.position();
            break;
         }
         if (chunks == 0) opensWithBlockData = chunk instanceof SBlockData;
         chunks++;
      }
      mark = rollback(mark, attempt, t, savedFields);
      // custom data precedes the defaults only when the whole section walks as custom data and opens with a
      // block-data record: when the walk fails, the opening "record" may be raw field values that merely look like one
      boolean customDataFirst = terminator >= 0 && opensWithBlockData;

      // the fields-first interpretation: when it parses, this attempt is also the commit
      if (!customDataFirst) {
         attempt = beginAttempt();
         if (parsesAsFieldsToEnd(t, cd, terminator)) {
            markedIn.releaseMark(mark);
            commitAttempt(attempt);
            return;
         }
         mark = rollback(mark, attempt, t, savedFields);
      }

      // the remaining interpretations: the fields starting after each of the leading custom-data chunks. The number
      // of chunks considered is capped so that a section of many chunks cannot drive unbounded re-parsing
      int interpretations = 0;
      int winner = -1;
      for (int leading = 1; leading <= Math.min(chunks, MAX_CUSTOM_DATA_PREFIX); leading++) {
         attempt = beginAttempt();
         if (!walkCustomDataChunks(leading)) {
            mark = rollback(mark, attempt, t, savedFields);
            break;
         }
         if (parsesAsFieldsToEnd(t, cd, terminator)) {
            interpretations++;
            winner = leading;
         }
         mark = rollback(mark, attempt, t, savedFields);
      }
      if (interpretations == 0 && customDataFirst) {
         // no layout with the fields after leading custom data parses: the opening block-data record may be raw
         // field data that the walk misread as a record, so try the fields at the section start after all
         attempt = beginAttempt();
         if (parsesAsFieldsToEnd(t, cd, terminator)) {
            markedIn.releaseMark(mark);
            commitAttempt(attempt);
            return;
         }
         mark = rollback(mark, attempt, t, savedFields);
      }
      if (interpretations == 1) {
         // re-parse the winning layout: the trials leave the stream and the parse state rolled back. The re-parse
         // runs outside any attempt, so its changes are permanent — or belong to the attempt of an enclosing
         // custom-writer section, when this slot was itself reached speculatively
         markedIn.resetToMark(mark);
         walkCustomDataChunks(winner);
         readDeclaredFields(t, cd);
         objectAnnotation(t);
      } else {
         // several layouts parse (ambiguous) or none does: render the section as custom data only rather than
         // presenting a speculative field layout as the correct one
         markedIn.resetToMark(mark);
         objectAnnotation(t); // if parsing the custom region also fails let it propagate: the stream is genuinely malformed
      }
   }

   /**
    * Rewinds to the given section mark and undoes the state changes of the speculative attempt (see
    * {@link #readCustomWriterSlot}). resetToMark consumes its token, so the mark is re-armed at the same position for
    * the next rewind.
    */
   private int rollback(int mark, Savepoint attempt, SObject t, Map<String, SEntity> savedFields) {
      markedIn.resetToMark(mark);
      undoLog.rollback(attempt);
      t.restoreFields(savedFields);
      return markedIn.mark();
   }

   /**
    * Consumes the given number of custom-data chunks (block-data records or nested objects) from the current
    * position. Returns false when the section cannot be walked that far, which ends the search for the start of the
    * declared fields.
    */
   private boolean walkCustomDataChunks(int count) {
      try {
         for (int i = 0; i < count; i++) {
            if (readObjectOrEnd() == END) return false;
         }
      } catch (Exception e) {
         return false;
      }
      return true;
   }

   /**
    * Tries one interpretation of a custom-writer section: the declared fields followed by the custom annotations,
    * from the current position. The interpretation parses when the fields and annotations consume without error
    * and, when the section's terminator is known, end exactly there: a parse ending elsewhere is reading a
    * block-data payload as values or running past the section into the following object.
    */
   private boolean parsesAsFieldsToEnd(SObject t, ObjectClassDesc cd, long terminator) {
      try {
         readDeclaredFields(t, cd);
         objectAnnotation(t);
      } catch (Exception e) {
         return false;
      }
      return terminator < 0 || markedIn.position() == terminator;
   }

   /**
    * Begins a speculative attempt (see {@link #readCustomWriterSlot}): mutations of the parse state are recorded
    * into the undo log, so that {@link #rollback} can undo exactly the changes this attempt made. Attempts nest —
    * a speculative parse can reach a nested custom-writer section — so each attempt opens a savepoint and only the
    * changes recorded after it are undone.
    */
   private Savepoint beginAttempt() {
      if (undoLog == null) undoLog = new UndoLog();
      return undoLog.begin();
   }

   /**
    * Ends a speculative attempt keeping its changes. They become permanent, unless an enclosing attempt is later
    * rolled back: the entries the attempt recorded stay in the enclosing attempt's undo range. When the last attempt
    * commits, the log itself is discarded and the changes are permanent outright.
    */
   private void commitAttempt(Savepoint attempt) {
      undoLog.commit(attempt);
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
         if (undoLog != null) undoLog.added(desc);
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
         if (undoLog != null) undoLog.added(desc);
         classDescriptions.put(desc.getName(), desc);
         return desc;
      }
   }

   private ObjectClassDesc newProxyClassDesc() throws IOException {
      ObjectClassDesc desc = new ObjectClassDesc("<Proxy>", SC_SERIALIZABLE, 1L);
      if (undoLog != null) undoLog.created(desc);
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
      if (size < 0 || size > MAX_ARRAY_SIZE) throw new StreamCorruptedException("Array size too large: " + size);
      // the elements are stored as they are read and SArray grows its backing store on demand: a size within the
      // limits can still overshoot the payload actually present, so allocating it upfront would let a truncated
      // stream exhaust the heap before the first element is even read
      SArray array = new SArray(classDesc.getType(), size);
      newHandle(array);
      ClassDesc componentClassDesc = classDesc.getComponentClassDesc();
      for (int i = 0; i < size; i++) {
         try {
            array.set(i, componentClassDesc.read());
         } catch (EOFException e) {
            // the declared size has no payload behind it: reject it as a corrupt stream rather than surfacing a bare
            // end-of-stream after the elements read so far
            throw new StreamCorruptedException("Truncated array: declared " + size + " elements, found " + i);
         }
      }
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
      // the descriptor's enum values are mutated below: capture its pristine state for a possible rollback
      if (undoLog != null) undoLog.capture(classDesc);
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

   private SBlockData blockData(long len) throws IOException {
      if (len < 0 || len > MAX_BLOCK_DATA_LENGTH) throw new StreamCorruptedException("Block data too large: " + len);
      // the payload is never rendered, only its length is: count the bytes off in bounded chunks instead of buffering
      // them, so a length within the limits cannot drive a large allocation before the payload is confirmed to exist
      byte[] chunk = new byte[(int) Math.min(len, BLOCK_DATA_CHUNK)];
      long remaining = len;
      while (remaining > 0) {
         int n = din.read(chunk, 0, (int) Math.min(chunk.length, remaining));
         if (n < 0) throw new StreamCorruptedException(
               "Truncated block data: declared " + len + " bytes, found " + (len - remaining));
         remaining -= n;
      }
      return new SBlockData(len);
   }

   private void newHandle(SEntity o) {
      handles.add(o);
   }

   private SEntity prevObject() throws IOException {
      int h = din.readInt();
      int i = h - baseWireHandle;
      int size = handles.size() - handleBase;
      if (i < 0 || i > size) throw new StreamCorruptedException("Bad handle: " + h);
      return handles.get(handleBase + i);
   }

   private void reset() {
      // the entries stay physically in the list so that a rollback can restore the table by truncating it: only the
      // logical base moves, after which prevObject resolves indices against the entries appended since (see UndoLog)
      handleBase = handles.size();
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
         if (undoLog != null) undoLog.capture(this);
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
         if (undoLog != null) undoLog.capture(this);
         this.fields = fields;
      }

      public int getFlags() {
         return flags;
      }

      public void setSuperClassDesc(ObjectClassDesc superClassDesc) {
         if (undoLog != null) undoLog.capture(this);
         this.superClassDesc = superClassDesc;
      }

      public List<ObjectClassDesc> getHierarchy() {
         if (hierarchy.isEmpty()) {
            // building the cache mutates this descriptor: capture its pristine (empty) state for a possible rollback
            if (undoLog != null) undoLog.capture(this);
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
         SEntity entity = readObject();
         // block data can never appear as a direct child of the instance-data region: encountering it here means we are misaligned (e.g. reading custom writeObject data as fields) so fail loudly rather than storing garbage
         if (entity instanceof SBlockData) throw new StreamCorruptedException("Unexpected block data in field value");
         return entity;
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

   /**
    * The undo log of the speculative parse attempts (see {@link Deserializer#readCustomWriterSlot}). Instead of
    * snapshotting the whole parse state per custom-writer section — which grows quadratically, since every section
    * would copy the handles and descriptors accumulated from all earlier objects — the log records only what the
    * attempts change, as they change it: the names added to the descriptor table, and the pristine state of a
    * pre-existing descriptor on its first mutation within an attempt. Attempts nest, so each opens a savepoint and
    * rolling back undoes only the entries recorded after it; a committed attempt leaves its entries in place for an
    * enclosing attempt to undo, and discarding the log when the last savepoint closes makes the changes permanent.
    * The handle table needs no entries at all: handles are only appended, undone by truncating to the savepoint's
    * size, and a TC_RESET moves the logical base of the table (see {@link Deserializer#reset()}) rather than
    * discarding the entries.
    */
   private final class UndoLog {

      // names added to the descriptor table, in the order the attempts added them
      private final List<String> addedNames = new ArrayList<>();
      // the pristine state of pre-existing descriptors, in the order the attempts first mutated them; a descriptor
      // can appear more than once, once per enclosing attempt, and the entries are undone innermost first
      private final List<DescriptorMutation> mutations = new ArrayList<>();
      // the savepoints of the attempts in progress, innermost last
      private final Deque<Savepoint> savepoints = new ArrayDeque<>();
      // descriptors created while the log is active: their table entries are removed on rollback and their own
      // mutations need no undo, since they become unreachable
      private final Set<ObjectClassDesc> created = Collections.newSetFromMap(new IdentityHashMap<>());

      // opens a savepoint for a new speculative attempt
      Savepoint begin() {
         Savepoint savepoint = new Savepoint(addedNames.size(), mutations.size(), handles.size(), handleBase);
         savepoints.push(savepoint);
         return savepoint;
      }

      // records a descriptor added to the descriptor table
      void added(ObjectClassDesc desc) {
         addedNames.add(desc.getName());
         created.add(desc);
      }

      // records a descriptor created outside the table, e.g. a proxy: it is only reachable through handles and
      // mutations to it need no undo
      void created(ObjectClassDesc desc) {
         created.add(desc);
      }

      // captures the pristine state of a pre-existing descriptor before the innermost attempt mutates it
      void capture(ObjectClassDesc desc) {
         Savepoint savepoint = savepoints.peek();
         if (savepoint == null || savepoint.captured.contains(desc) || created.contains(desc)) return;
         savepoint.captured.add(desc);
         mutations.add(new DescriptorMutation(desc, desc.fields, desc.serialVersionUID, desc.superClassDesc,
               new ArrayList<>(desc.hierarchy), new HashSet<>(desc.enumValues)));
      }

      // undoes everything recorded since the given savepoint, and drops the savepoint
      void rollback(Savepoint savepoint) {
         for (int i = mutations.size() - 1; i >= savepoint.mutations; i--) mutations.get(i).restore();
         for (int i = addedNames.size() - 1; i >= savepoint.addedNames; i--) classDescriptions.remove(addedNames.get(i));
         if (handles.size() > savepoint.handleSize) handles.subList(savepoint.handleSize, handles.size()).clear();
         handleBase = savepoint.handleBase;
         mutations.subList(savepoint.mutations, mutations.size()).clear();
         addedNames.subList(savepoint.addedNames, addedNames.size()).clear();
         savepoints.pop();
         if (savepoints.isEmpty()) undoLog = null;
      }

      // drops the given savepoint keeping its entries: they are undone by an enclosing attempt's rollback, or
      // become permanent when the last savepoint closes and the log is discarded
      void commit(Savepoint savepoint) {
         savepoints.pop();
         if (savepoints.isEmpty()) undoLog = null;
      }
   }

   // the position of a speculative attempt in the undo log (see UndoLog)
   private static final class Savepoint {
      // the undo log length when the attempt began
      final int addedNames;
      final int mutations;
      // the handle table when the attempt began: entries appended since are truncated away
      final int handleSize;
      final int handleBase;
      // the descriptors this attempt has captured, so each is captured at most once per attempt
      final Set<ObjectClassDesc> captured = Collections.newSetFromMap(new IdentityHashMap<>());

      Savepoint(int addedNames, int mutations, int handleSize, int handleBase) {
         this.addedNames = addedNames;
         this.mutations = mutations;
         this.handleSize = handleSize;
         this.handleBase = handleBase;
      }
   }

   // the pristine state of a descriptor, from which it is restored on rollback (see UndoLog)
   private static final class DescriptorMutation {
      private final ObjectClassDesc desc;
      private final FieldDesc[] fields;
      private final Long serialVersionUID;
      private final ObjectClassDesc superClassDesc;
      private final List<ObjectClassDesc> hierarchy;
      private final Set<String> enumValues;

      DescriptorMutation(ObjectClassDesc desc, FieldDesc[] fields, Long serialVersionUID, ObjectClassDesc superClassDesc, List<ObjectClassDesc> hierarchy, Set<String> enumValues) {
         this.desc = desc;
         this.fields = fields;
         this.serialVersionUID = serialVersionUID;
         this.superClassDesc = superClassDesc;
         this.hierarchy = hierarchy;
         this.enumValues = enumValues;
      }

      void restore() {
         desc.fields = fields;
         desc.serialVersionUID = serialVersionUID;
         desc.superClassDesc = superClassDesc;
         desc.hierarchy.clear();
         desc.hierarchy.addAll(hierarchy);
         desc.enumValues.clear();
         desc.enumValues.addAll(enumValues);
      }
   }

   /**
    * An {@link InputStream} supporting nested mark/reset by replaying the bytes read since the requested {@code mark()} call. Marks form a stack: each {@code mark()} records the current stream position and returns a token, {@code resetToMark(int)} rewinds to that specific mark, discarding any newer marks created in the meantime (for example by a nested custom writer), and {@code releaseMark(int)} drops a mark whose section was consumed without a rewind. Bytes are only captured while at least one mark is outstanding or a replay is being served, so memory usage is bounded by the bytes read while a mark is outstanding rather than by the size of the whole stream.
    */
   private static final class MarkedStream extends FilterInputStream {

      private final Deque<Integer> marks = new ArrayDeque<>();
      private final Map<Integer, Long> markPositions = new HashMap<>();
      private int nextToken = 1;
      // total bytes read from the underlying stream, in stream coordinates (monotonically increasing; the position seen by the consumer can be rewound)
      private long streamPos;
      // while a mark is outstanding, the bytes read from the underlying stream since captureBase are retained so a later resetToMark can replay them
      private byte[] capture;
      private long captureBase;
      private int captureSize;
      // snapshot served after resetToMark() before falling through to the underlying stream again; replayStart is its position in stream coordinates
      private byte[] replay;
      private long replayStart;
      private int replayPos;

      MarkedStream(InputStream in) {
         super(in);
      }

      int mark() {
         // while a replay from an earlier reset is being served the consumer position lags behind the underlying stream, so the mark must record the logical position for a later resetToMark to rewind to the right place
         long pos = (replay != null && replayPos < replay.length) ? replayStart + replayPos : streamPos;
         if (capture == null) {
            captureBase = streamPos;
            capture = new byte[256];
            captureSize = 0;
         }
         int token = nextToken++;
         marks.push(token);
         markPositions.put(token, pos);
         return token;
      }

      // the logical position of the consumer in stream coordinates (see mark())
      long position() {
         return (replay != null && replayPos < replay.length) ? replayStart + replayPos : streamPos;
      }

      /**
       * Drops a mark whose section was consumed successfully, without rewinding. Frees the capture buffer when no marks remain.
       */
      void releaseMark(int token) {
         popMarksAbove(token);
         if (marks.isEmpty() || marks.peek() != token) throw new IllegalStateException("Unknown or already released mark: " + token);
         marks.pop();
         markPositions.remove(token);
         releaseCaptureIfPossible();
      }

      void resetToMark(int token) {
         popMarksAbove(token);
         if (marks.isEmpty() || marks.peek() != token) throw new IllegalStateException("Unknown or already consumed mark: " + token);
         long pos = markPositions.remove(token);
         marks.pop();
         // The bytes read since the mark can span at most two stored regions: a pending replay (when the mark was created while a previous replay was being served) and the capture buffer
         byte[] newReplay;
         if (replay != null && replayPos < replay.length && pos >= replayStart) {
            int firstLen = replay.length - (int) (pos - replayStart);
            // the capture buffer may predate the replay (held open by an older mark): in that case only the tail beyond the replay is needed
            int secondStart = (int) Math.max(0, replayStart + replay.length - captureBase);
            int secondLen = captureSize - secondStart;
            newReplay = new byte[firstLen + secondLen];
            System.arraycopy(replay, (int) (pos - replayStart), newReplay, 0, firstLen);
            if (secondLen > 0) System.arraycopy(capture, secondStart, newReplay, firstLen, secondLen);
         } else {
            int rel = (int) (pos - captureBase);
            newReplay = Arrays.copyOfRange(capture, rel, captureSize);
         }
         this.replay = newReplay;
         this.replayStart = pos;
         this.replayPos = 0;
         releaseCaptureIfPossible();
      }

      // the capture must outlive any pending replay: a mark created while a replay is being served may need to rewind to a position the replay covers, and the capture buffer may not have started before that position
      private void releaseCaptureIfPossible() {
         if (marks.isEmpty() && (replay == null || replayPos >= replay.length)) capture = null;
      }

      @Override
      public int read() throws IOException {
         if (replay != null) {
            if (replayPos < replay.length) return replay[replayPos++] & 0xff;
            // the replay is fully drained: no outstanding mark needs it, so drop it and free the capture buffer with it
            replay = null;
            releaseCaptureIfPossible();
         }
         int b = in.read();
         if (b >= 0) {
            streamPos++;
            captureByte((byte) b);
         }
         return b;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         if (replay != null) {
            if (replayPos < replay.length) {
               int n = Math.min(len, replay.length - replayPos);
               System.arraycopy(replay, replayPos, b, off, n);
               replayPos += n;
               return n;
            }
            // the replay is fully drained: no outstanding mark needs it, so drop it and free the capture buffer with it
            replay = null;
            releaseCaptureIfPossible();
         }
         int r = in.read(b, off, len);
         if (r > 0) {
            streamPos += r;
            captureBytes(b, off, r);
         }
         return r;
      }

      private void captureByte(byte b) {
         if (capture == null) return;
         if (captureSize == capture.length) capture = Arrays.copyOf(capture, capture.length * 2);
         capture[captureSize++] = b;
      }

      private void captureBytes(byte[] b, int off, int len) {
         if (capture == null || len == 0) return;
         int newSize = captureSize + len;
         if (newSize > capture.length) capture = Arrays.copyOf(capture, Math.max(newSize, capture.length * 2));
         System.arraycopy(b, off, capture, captureSize, len);
         captureSize = newSize;
      }

      private void popMarksAbove(int token) {
         Integer top;
         while ((top = marks.peek()) != null && top != token) {
            marks.pop();
            markPositions.remove(top);
         }
      }
   }
}
