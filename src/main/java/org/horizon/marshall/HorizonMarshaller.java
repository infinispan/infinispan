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
package org.horizon.marshall;

import org.horizon.CacheException;
import org.horizon.atomic.DeltaAware;
import org.horizon.commands.RemoteCommandFactory;
import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.write.WriteCommand;
import org.horizon.io.ByteBuffer;
import org.horizon.io.ExposedByteArrayOutputStream;
import org.horizon.loader.StoredEntry;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.remoting.transport.Address;
import org.horizon.remoting.transport.jgroups.ExtendedResponse;
import org.horizon.remoting.transport.jgroups.JGroupsAddress;
import org.horizon.remoting.transport.jgroups.RequestIgnoredResponse;
import org.horizon.transaction.GlobalTransaction;
import org.horizon.transaction.TransactionLog;
import org.horizon.util.FastCopyHashMap;
import org.horizon.util.Immutables;
import org.jboss.util.NotImplementedException;
import org.jboss.util.stream.MarshalledValueInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Abstract marshaller
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class HorizonMarshaller implements Marshaller {
   // magic numbers
   protected static final int MAGICNUMBER_GTX = 1;
   protected static final int MAGICNUMBER_JG_ADDRESS = 2;
   protected static final int MAGICNUMBER_ARRAY_LIST = 3;
   protected static final int MAGICNUMBER_INTEGER = 4;
   protected static final int MAGICNUMBER_LONG = 5;
   protected static final int MAGICNUMBER_BOOLEAN = 6;
   protected static final int MAGICNUMBER_STRING = 7;
   protected static final int MAGICNUMBER_LINKED_LIST = 8;
   protected static final int MAGICNUMBER_HASH_MAP = 9;
   protected static final int MAGICNUMBER_TREE_MAP = 10;
   protected static final int MAGICNUMBER_HASH_SET = 11;
   protected static final int MAGICNUMBER_TREE_SET = 12;
   protected static final int MAGICNUMBER_SHORT = 13;
   protected static final int MAGICNUMBER_IMMUTABLE_MAPCOPY = 14;
   protected static final int MAGICNUMBER_MARSHALLEDVALUE = 15;
   protected static final int MAGICNUMBER_FASTCOPY_HASHMAP = 16;
   protected static final int MAGICNUMBER_ARRAY = 17;
   protected static final int MAGICNUMBER_BYTE = 18;
   protected static final int MAGICNUMBER_CHAR = 19;
   protected static final int MAGICNUMBER_FLOAT = 20;
   protected static final int MAGICNUMBER_DOUBLE = 21;
   protected static final int MAGICNUMBER_OBJECT = 22;
   protected static final int MAGICNUMBER_SINGLETON_LIST = 23;
   protected static final int MAGICNUMBER_COMMAND = 24;
   protected static final int MAGICNUMBER_TRANSACTION_LOG = 25;
   protected static final int MAGICNUMBER_STORED_ENTRY = 26;
   protected static final int MAGICNUMBER_REQUEST_IGNORED_RESPONSE = 27;
   protected static final int MAGICNUMBER_EXTENDED_RESPONSE = 28;   
   protected static final int MAGICNUMBER_NULL = 99;
   protected static final int MAGICNUMBER_SERIALIZABLE = 100;
   protected static final int MAGICNUMBER_REF = 101;

   public HorizonMarshaller() {
      initLogger();
      // enabled, since this is always enabled in JBC 2.0.0.
      useRefs = false;
   }

   protected Log log;
   protected boolean trace;
   private RemoteCommandFactory remoteCommandFactory;
   protected ClassLoader defaultClassLoader;
   protected boolean useRefs = false;

   public void init(ClassLoader defaultClassLoader, RemoteCommandFactory remoteCommandFactory) {
      this.defaultClassLoader = defaultClassLoader;
      this.remoteCommandFactory = remoteCommandFactory;
   }

   protected void initLogger() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   public byte[] objectToByteBuffer(Object obj) throws IOException {
      ByteBuffer b = objectToBuffer(obj);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   protected void marshallObject(Object o, ObjectOutput out, Map<Object, Integer> refMap) throws IOException {
      if (o != null && o.getClass().isArray() && isKnownType(o.getClass().getComponentType())) {
         marshallArray(o, out, refMap);
      } else {
         if (o == null) {
            out.writeByte(MAGICNUMBER_NULL);
         } else if (useRefs && refMap.containsKey(o))// see if this object has been marshalled before.
         {
            out.writeByte(MAGICNUMBER_REF);
            writeReference(out, refMap.get(o));
         } else if (o instanceof ReplicableCommand) {
            ReplicableCommand command = (ReplicableCommand) o;

            if (command.getCommandId() > -1) {
               out.writeByte(MAGICNUMBER_COMMAND);
               marshallCommand(command, out, refMap);
            } else {
               throw new IllegalArgumentException("Command does not have a valid method id!");
            }
         } else if (o instanceof MarshalledValue) {
            out.writeByte(MAGICNUMBER_MARSHALLEDVALUE);
            ((MarshalledValue) o).writeExternal(out);
         } else if (o instanceof DeltaAware) {
            // reading in should be nothing special.
            out.writeByte(MAGICNUMBER_SERIALIZABLE);
            // only write the delta for these maps.
            out.writeObject(((DeltaAware) o).delta());
         } else if (o instanceof GlobalTransaction) {
            out.writeByte(MAGICNUMBER_GTX);
            if (useRefs) writeReference(out, createReference(o, refMap));
            marshallGlobalTransaction((GlobalTransaction) o, out, refMap);
         } else if (o instanceof JGroupsAddress) {
            out.writeByte(MAGICNUMBER_JG_ADDRESS);
            marshallJGroupsAddress((JGroupsAddress) o, out);
         } else if (o instanceof RequestIgnoredResponse) {
            out.writeByte(MAGICNUMBER_REQUEST_IGNORED_RESPONSE);
         } else if (o instanceof ExtendedResponse) {
            out.writeByte(MAGICNUMBER_EXTENDED_RESPONSE);
            ExtendedResponse er = (ExtendedResponse) o;
            out.writeBoolean(er.isReplayIgnoredRequests());
            marshallObject(er.getResponse(), out, refMap);
         } else if (o instanceof StoredEntry) {
            out.writeByte(MAGICNUMBER_STORED_ENTRY);
            ((StoredEntry) o).writeExternal(out);
         } else if (o.getClass().equals(ArrayList.class)) {
            out.writeByte(MAGICNUMBER_ARRAY_LIST);
            marshallCollection((Collection) o, out, refMap);
         } else if (o instanceof LinkedList) {
            out.writeByte(MAGICNUMBER_LINKED_LIST);
            marshallCollection((Collection) o, out, refMap);
         } else if (o.getClass().getName().equals("java.util.Collections$SingletonList")) {
            out.writeByte(MAGICNUMBER_SINGLETON_LIST);
            marshallObject(((List) o).get(0), out, refMap);
         } else if (o.getClass().equals(HashMap.class)) {
            out.writeByte(MAGICNUMBER_HASH_MAP);
            marshallMap((Map) o, out, refMap);
         } else if (o.getClass().equals(TreeMap.class)) {
            out.writeByte(MAGICNUMBER_TREE_MAP);
            marshallMap((Map) o, out, refMap);
         } else if (o.getClass().equals(FastCopyHashMap.class)) {
            out.writeByte(MAGICNUMBER_FASTCOPY_HASHMAP);
            marshallMap((Map) o, out, refMap);
         } else if (o instanceof Map && Immutables.isImmutable(o)) {
            out.writeByte(MAGICNUMBER_IMMUTABLE_MAPCOPY);
            marshallMap((Map) o, out, refMap);
         } else if (o.getClass().equals(HashSet.class)) {
            out.writeByte(MAGICNUMBER_HASH_SET);
            marshallCollection((Collection) o, out, refMap);
         } else if (o.getClass().equals(TreeSet.class)) {
            out.writeByte(MAGICNUMBER_TREE_SET);
            marshallCollection((Collection) o, out, refMap);
         } else if (o instanceof Boolean) {
            out.writeByte(MAGICNUMBER_BOOLEAN);
            out.writeBoolean(((Boolean) o).booleanValue());
         } else if (o instanceof Integer) {
            out.writeByte(MAGICNUMBER_INTEGER);
            out.writeInt(((Integer) o).intValue());
         } else if (o instanceof Long) {
            out.writeByte(MAGICNUMBER_LONG);
            out.writeLong(((Long) o).longValue());
         } else if (o instanceof Short) {
            out.writeByte(MAGICNUMBER_SHORT);
            out.writeShort(((Short) o).shortValue());
         } else if (o instanceof String) {
            out.writeByte(MAGICNUMBER_STRING);
            if (useRefs) writeReference(out, createReference(o, refMap));
            marshallString((String) o, out);
         } else if (o instanceof TransactionLog.LogEntry) {
            out.writeByte(MAGICNUMBER_TRANSACTION_LOG);
            TransactionLog.LogEntry le = (TransactionLog.LogEntry) o;
            marshallObject(le.getTransaction(), out, refMap);
            WriteCommand[] cmds = le.getModifications();
            writeUnsignedInt(out, cmds.length);
            for (WriteCommand c : cmds)
               marshallObject(c, out, refMap);
         } else if (o instanceof Serializable) {
            if (trace) log.trace("WARNING: using object serialization for [{0}]", o.getClass());

            out.writeByte(MAGICNUMBER_SERIALIZABLE);
            if (useRefs) writeReference(out, createReference(o, refMap));
            out.writeObject(o);
         } else {
            throw new IOException("Don't know how to marshall object of type " + o.getClass());
         }
      }
   }


   protected void marshallString(String s, ObjectOutput out) throws IOException {
      //StringUtil.saveString(out, s);
      out.writeObject(s);
   }

   private void marshallCommand(ReplicableCommand command, ObjectOutput out, Map<Object, Integer> refMap) throws IOException {
      out.writeShort(command.getCommandId());
      Object[] args = command.getParameters();
      byte numArgs = (byte) (args == null ? 0 : args.length);
      out.writeByte(numArgs);

      for (int i = 0; i < numArgs; i++) {
         marshallObject(args[i], out, refMap);
      }
   }

   private int createReference(Object o, Map<Object, Integer> refMap) {
      int reference = refMap.size();
      refMap.put(o, reference);
      return reference;
   }

   private void marshallGlobalTransaction(GlobalTransaction globalTransaction, ObjectOutput out, Map<Object, Integer> refMap) throws IOException {
      out.writeLong(globalTransaction.getId());
      marshallObject(globalTransaction.getAddress(), out, refMap);
   }

   private void marshallJGroupsAddress(JGroupsAddress address, ObjectOutput out) throws IOException {
      address.writeExternal(out);
   }

   @SuppressWarnings("unchecked")
   private void marshallCollection(Collection c, ObjectOutput out, Map refMap) throws IOException {
      writeUnsignedInt(out, c.size());
      for (Object o : c) {
         marshallObject(o, out, refMap);
      }
   }

   @SuppressWarnings("unchecked")
   private void marshallMap(Map map, ObjectOutput out, Map<Object, Integer> refMap) throws IOException {
      int mapSize = map.size();
      writeUnsignedInt(out, mapSize);
      if (mapSize == 0) return;

      for (Map.Entry me : (Set<Map.Entry>) map.entrySet()) {
         marshallObject(me.getKey(), out, refMap);
         marshallObject(me.getValue(), out, refMap);
      }
   }

   // --------- Unmarshalling methods

   protected Object unmarshallObject(ObjectInput in, ClassLoader loader, UnmarshalledReferences refMap, boolean overrideContextClassloaderOnThread) throws IOException, ClassNotFoundException {
      if (loader == null) {
         return unmarshallObject(in, refMap);
      } else {
         Thread currentThread = Thread.currentThread();
         ClassLoader old = currentThread.getContextClassLoader();
         try {
            // only do this if we haven't already set a context class loaderold elsewhere.
            if (overrideContextClassloaderOnThread || old == null) currentThread.setContextClassLoader(loader);
            return unmarshallObject(in, refMap);
         }
         finally {
            if (overrideContextClassloaderOnThread || old == null) currentThread.setContextClassLoader(old);
         }
      }
   }

   protected Object unmarshallObject(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      byte magicNumber = in.readByte();
      int reference = 0;
      Object retVal;
      switch (magicNumber) {
         case MAGICNUMBER_NULL:
            return null;
         case MAGICNUMBER_REF:
            if (useRefs) {
               reference = readReference(in);
               return refMap.getReferencedObject(reference);
            } else break;
         case MAGICNUMBER_SERIALIZABLE:
            if (useRefs) reference = readReference(in);
            retVal = in.readObject();
            if (useRefs) refMap.putReferencedObject(reference, retVal);
            return retVal;
         case MAGICNUMBER_MARSHALLEDVALUE:
            MarshalledValue mv = new MarshalledValue();
            mv.readExternal(in);
            return mv;
         case MAGICNUMBER_REQUEST_IGNORED_RESPONSE:
            return RequestIgnoredResponse.INSTANCE;
         case MAGICNUMBER_EXTENDED_RESPONSE:
            boolean replayIgnoredRequests = in.readBoolean();
            Object response = unmarshallObject(in, refMap);
            return new ExtendedResponse(response, replayIgnoredRequests);
         case MAGICNUMBER_STORED_ENTRY:
            StoredEntry se = new StoredEntry();
            se.readExternal(in);
            return se;
         case MAGICNUMBER_COMMAND:
            retVal = unmarshallCommand(in, refMap);
            return retVal;
         case MAGICNUMBER_GTX:
            if (useRefs) reference = readReference(in);
            retVal = unmarshallGlobalTransaction(in, refMap);
            if (useRefs) refMap.putReferencedObject(reference, retVal);
            return retVal;
         case MAGICNUMBER_JG_ADDRESS:
            retVal = unmarshallJGroupsAddress(in);
            return retVal;
         case MAGICNUMBER_TRANSACTION_LOG:
            GlobalTransaction gtx = (GlobalTransaction) unmarshallObject(in, refMap);
            int numCommands = readUnsignedInt(in);
            WriteCommand[] cmds = new WriteCommand[numCommands];
            for (int i = 0; i < numCommands; i++) cmds[i] = (WriteCommand) unmarshallObject(in, refMap);
            return new TransactionLog.LogEntry(gtx, cmds);
         case MAGICNUMBER_ARRAY:
            return unmarshallArray(in, refMap);
         case MAGICNUMBER_ARRAY_LIST:
            return unmarshallArrayList(in, refMap);
         case MAGICNUMBER_LINKED_LIST:
            return unmarshallLinkedList(in, refMap);
         case MAGICNUMBER_SINGLETON_LIST:
            return unmarshallSingletonList(in, refMap);
         case MAGICNUMBER_HASH_MAP:
            return unmarshallHashMap(in, refMap);
         case MAGICNUMBER_TREE_MAP:
            return unmarshallTreeMap(in, refMap);
         case MAGICNUMBER_HASH_SET:
            return unmarshallHashSet(in, refMap);
         case MAGICNUMBER_TREE_SET:
            return unmarshallTreeSet(in, refMap);
         case MAGICNUMBER_IMMUTABLE_MAPCOPY:
            return unmarshallMapCopy(in, refMap);
         case MAGICNUMBER_FASTCOPY_HASHMAP:
            return unmarshallFastCopyHashMap(in, refMap);
         case MAGICNUMBER_BOOLEAN:
            return in.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
         case MAGICNUMBER_INTEGER:
            return in.readInt();
         case MAGICNUMBER_LONG:
            return in.readLong();
         case MAGICNUMBER_SHORT:
            return in.readShort();
         case MAGICNUMBER_STRING:
            if (useRefs) reference = readReference(in);
            retVal = unmarshallString(in);
            if (useRefs) refMap.putReferencedObject(reference, retVal);
            return retVal;
         default:
            if (log.isErrorEnabled()) {
               log.error("Unknown Magic Number " + magicNumber);
            }
            throw new IOException("Unknown magic number " + magicNumber);
      }
      throw new IOException("Unknown magic number " + magicNumber);
   }

   private FastCopyHashMap unmarshallFastCopyHashMap(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      FastCopyHashMap map = new FastCopyHashMap();
      populateFromStream(in, refMap, map);
      return map;
   }

   protected String unmarshallString(ObjectInput in) throws IOException, ClassNotFoundException {
      return (String) in.readObject();
   }

   private ReplicableCommand unmarshallCommand(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      short methodId = in.readShort();
      byte numArgs = in.readByte();
      Object[] args = null;

      if (numArgs > 0) {
         args = new Object[numArgs];
         for (int i = 0; i < numArgs; i++) args[i] = unmarshallObject(in, refMap);
      }

      return remoteCommandFactory.fromStream((byte) methodId, args);
   }


   private GlobalTransaction unmarshallGlobalTransaction(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      GlobalTransaction gtx = new GlobalTransaction();
      long id = in.readLong();
      Object address = unmarshallObject(in, refMap);
      gtx.setId(id);
      gtx.setAddress((Address) address);
      return gtx;
   }

   private JGroupsAddress unmarshallJGroupsAddress(ObjectInput in) throws IOException, ClassNotFoundException {
      JGroupsAddress address = new JGroupsAddress();
      address.readExternal(in);
      return address;
   }

   private List unmarshallArrayList(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      int listSize = readUnsignedInt(in);
      List list = new ArrayList(listSize);
      populateFromStream(in, refMap, list, listSize);
      return list;
   }

   private List unmarshallLinkedList(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      List list = new LinkedList();
      populateFromStream(in, refMap, list, readUnsignedInt(in));
      return list;
   }

   private List unmarshallSingletonList(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      return Collections.singletonList(unmarshallObject(in, refMap));
   }

   private Map unmarshallHashMap(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      Map map = new HashMap();
      populateFromStream(in, refMap, map);
      return map;
   }

   @SuppressWarnings("unchecked")
   private Map unmarshallMapCopy(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      // read in as a HashMap first
      Map m = unmarshallHashMap(in, refMap);
      return Immutables.immutableMapWrap(m);
   }

   private Map unmarshallTreeMap(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      Map map = new TreeMap();
      populateFromStream(in, refMap, map);
      return map;
   }

   private Set unmarshallHashSet(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      Set set = new HashSet();
      populateFromStream(in, refMap, set);
      return set;
   }

   private Set unmarshallTreeSet(ObjectInput in, UnmarshalledReferences refMap) throws IOException, ClassNotFoundException {
      Set set = new TreeSet();
      populateFromStream(in, refMap, set);
      return set;
   }

   @SuppressWarnings("unchecked")
   private void populateFromStream(ObjectInput in, UnmarshalledReferences refMap, Map mapToPopulate) throws IOException, ClassNotFoundException {
      int size = readUnsignedInt(in);
      for (int i = 0; i < size; i++) mapToPopulate.put(unmarshallObject(in, refMap), unmarshallObject(in, refMap));
   }

   @SuppressWarnings("unchecked")
   private void populateFromStream(ObjectInput in, UnmarshalledReferences refMap, Set setToPopulate) throws IOException, ClassNotFoundException {
      int size = readUnsignedInt(in);
      for (int i = 0; i < size; i++) setToPopulate.add(unmarshallObject(in, refMap));
   }

   @SuppressWarnings("unchecked")
   private void populateFromStream(ObjectInput in, UnmarshalledReferences refMap, List listToPopulate, int listSize) throws IOException, ClassNotFoundException {
      for (int i = 0; i < listSize; i++) listToPopulate.add(unmarshallObject(in, refMap));
   }

   /**
    * This version of writeReference is written to solve JBCACHE-1211, where references are encoded as ints rather than
    * shorts.
    *
    * @param out       stream to write to
    * @param reference reference to write
    * @throws java.io.IOException propagated from OOS
    * @see <a href="http://jira.jboss.org/jira/browse/JBCACHE-1211">JBCACHE-1211</a>
    */
   protected void writeReference(ObjectOutput out, int reference) throws IOException {
      writeUnsignedInt(out, reference);
   }

   /**
    * This version of readReference is written to solve JBCACHE-1211, where references are encoded as ints rather than
    * shorts.
    *
    * @param in stream to read from
    * @return reference
    * @throws java.io.IOException propagated from OUS
    * @see <a href="http://jira.jboss.org/jira/browse/JBCACHE-1211">JBCACHE-1211</a>
    */
   protected int readReference(ObjectInput in) throws IOException {
      return readUnsignedInt(in);
   }

   /**
    * Reads an int stored in variable-length format.  Reads between one and five bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   protected int readUnsignedInt(ObjectInput in) throws IOException {
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Writes an int in a variable-length format.  Writes between one and five bytes.  Smaller values take fewer bytes.
    * Negative numbers are not supported.
    *
    * @param i int to write
    */
   protected void writeUnsignedInt(ObjectOutput out, int i) throws IOException {
      while ((i & ~0x7F) != 0) {
         out.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      out.writeByte((byte) i);
   }


   /**
    * Reads an int stored in variable-length format.  Reads between one and nine bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   protected long readUnsignedLong(ObjectInput in) throws IOException {
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Writes an int in a variable-length format.  Writes between one and nine bytes.  Smaller values take fewer bytes.
    * Negative numbers are not supported.
    *
    * @param i int to write
    */
   protected void writeUnsignedLong(ObjectOutput out, long i) throws IOException {
      while ((i & ~0x7F) != 0) {
         out.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      out.writeByte((byte) i);
   }

   protected Object unmarshallArray(ObjectInput in, UnmarshalledReferences refs) throws IOException, ClassNotFoundException {
      int sz = readUnsignedInt(in);
      byte type = in.readByte();
      switch (type) {
         case MAGICNUMBER_BOOLEAN: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               boolean[] a = new boolean[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readBoolean();
               return a;
            } else {
               Boolean[] a = new Boolean[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readBoolean();
               return a;
            }
         }
         case MAGICNUMBER_INTEGER: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               int[] a = new int[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readInt();
               return a;
            } else {
               Integer[] a = new Integer[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readInt();
               return a;
            }
         }
         case MAGICNUMBER_LONG: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               long[] a = new long[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readLong();
               return a;
            } else {
               Long[] a = new Long[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readLong();
               return a;
            }
         }
         case MAGICNUMBER_CHAR: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               char[] a = new char[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readChar();
               return a;
            } else {
               Character[] a = new Character[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readChar();
               return a;
            }
         }
         case MAGICNUMBER_BYTE: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               byte[] a = new byte[sz];
               int bsize = 10240;
               int offset = 0;
               int bytesLeft = sz;
               while (bytesLeft > 0) {
                  int read = in.read(a, offset, Math.min(bsize, bytesLeft));
                  offset += read;
                  bytesLeft -= read;
               }
               return a;
            } else {
               Byte[] a = new Byte[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readByte();
               return a;
            }
         }
         case MAGICNUMBER_SHORT: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               short[] a = new short[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readShort();
               return a;
            } else {
               Short[] a = new Short[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readShort();
               return a;
            }
         }
         case MAGICNUMBER_FLOAT: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               float[] a = new float[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readFloat();
               return a;
            } else {
               Float[] a = new Float[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readFloat();
               return a;
            }
         }
         case MAGICNUMBER_DOUBLE: {
            boolean isPrim = in.readBoolean();
            if (isPrim) {
               double[] a = new double[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readDouble();
               return a;
            } else {
               Double[] a = new Double[sz];
               for (int i = 0; i < sz; i++) a[i] = in.readDouble();
               return a;
            }
         }
         case MAGICNUMBER_OBJECT: {
            Object[] a = new Object[sz];
            for (int i = 0; i < sz; i++) a[i] = unmarshallObject(in, refs);
            return a;
         }
         default:
            throw new CacheException("Unknown array type");
      }
   }

   protected void marshallArray(Object o, ObjectOutput out, Map<Object, Integer> refMap) throws IOException {
      out.writeByte(MAGICNUMBER_ARRAY);
      Class arrayTypeClass = o.getClass().getComponentType();
      int sz = Array.getLength(o);
      writeUnsignedInt(out, sz);
      boolean isPrim = arrayTypeClass.isPrimitive();

      if (!isPrim && arrayTypeClass.equals(Object.class)) {
         out.writeByte(MAGICNUMBER_OBJECT);
         for (int i = 0; i < sz; i++) marshallObject(Array.get(o, i), out, refMap);
      } else if (arrayTypeClass.equals(byte.class) || arrayTypeClass.equals(Byte.class)) {
         out.writeByte(MAGICNUMBER_BYTE);
         out.writeBoolean(isPrim);
         if (isPrim)
            out.write((byte[]) o);
         else
            for (int i = 0; i < sz; i++) out.writeByte((Byte) Array.get(o, i));
      } else if (arrayTypeClass.equals(int.class) || arrayTypeClass.equals(Integer.class)) {
         out.writeByte(MAGICNUMBER_INTEGER);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeInt(Array.getInt(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeInt((Integer) Array.get(o, i));
      } else if (arrayTypeClass.equals(long.class) || arrayTypeClass.equals(Long.class)) {
         out.writeByte(MAGICNUMBER_LONG);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeLong(Array.getLong(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeLong((Long) Array.get(o, i));
      } else if (arrayTypeClass.equals(boolean.class) || arrayTypeClass.equals(Boolean.class)) {
         out.writeByte(MAGICNUMBER_BOOLEAN);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeBoolean(Array.getBoolean(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeBoolean((Boolean) Array.get(o, i));
      } else if (arrayTypeClass.equals(char.class) || arrayTypeClass.equals(Character.class)) {
         out.writeByte(MAGICNUMBER_CHAR);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeChar(Array.getChar(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeChar((Character) Array.get(o, i));
      } else if (arrayTypeClass.equals(short.class) || arrayTypeClass.equals(Short.class)) {
         out.writeByte(MAGICNUMBER_SHORT);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeShort(Array.getShort(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeShort((Short) Array.get(o, i));
      } else if (arrayTypeClass.equals(float.class) || arrayTypeClass.equals(Float.class)) {
         out.writeByte(MAGICNUMBER_FLOAT);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeFloat(Array.getFloat(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeFloat((Float) Array.get(o, i));
      } else if (arrayTypeClass.equals(double.class) || arrayTypeClass.equals(Double.class)) {
         out.writeByte(MAGICNUMBER_DOUBLE);
         out.writeBoolean(isPrim);
         if (isPrim)
            for (int i = 0; i < sz; i++) out.writeDouble(Array.getDouble(o, i));
         else
            for (int i = 0; i < sz; i++) out.writeDouble((Double) Array.get(o, i));
      } else throw new CacheException("Unknown array type!");
   }

   protected boolean isKnownType(Class c) {
      return (c.equals(Object.class) ||
            c.isPrimitive() || c.equals(Character.class) || c.equals(Integer.class) || c.equals(Long.class) ||
            c.equals(Byte.class) || c.equals(Boolean.class) || c.equals(Short.class) || c.equals(Float.class) ||
            c.equals(Double.class));
   }

   public void objectToObjectStream(Object o, ObjectOutput out) throws IOException {
      Map<Object, Integer> refMap = useRefs ? new IdentityHashMap<Object, Integer>() : null;
      ClassLoader toUse = defaultClassLoader;
      Thread current = Thread.currentThread();
      ClassLoader old = current.getContextClassLoader();
      if (old != null) toUse = old;

      try {
         current.setContextClassLoader(toUse);
         marshallObject(o, out, refMap);
      }
      finally {
         current.setContextClassLoader(old);
      }
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      UnmarshalledReferences refMap = useRefs ? new UnmarshalledReferences() : null;
      Object retValue = unmarshallObject(in, defaultClassLoader, refMap, false);
      if (trace) log.trace("Unmarshalled object " + retValue);
      return retValue;
   }

   public Object objectFromStream(InputStream is) throws IOException {
      throw new NotImplementedException("not implemented");
   }

   public ByteBuffer objectToBuffer(Object o) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(128);
      ObjectOutput out = new ObjectOutputStream(baos);

      //now marshall the contents of the object
      objectToObjectStream(o, out);
      out.close();
      // and return bytes.
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      ObjectInput in = new MarshalledValueInputStream(new ByteArrayInputStream(buf, offset, length));
      return objectFromObjectStream(in);
   }

   public Object objectFromByteBuffer(byte[] bytes) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(bytes, 0, bytes.length);
   }
}
