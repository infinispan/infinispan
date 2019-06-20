package org.infinispan.marshall.exts;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.util.Util;
import org.infinispan.interceptors.distribution.ConcurrentChangeException;
import org.infinispan.interceptors.impl.ContainerFullException;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.marshall.core.Ids;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.remoting.CacheUnreachableException;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheJoinException;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.UserRaisedFunctionalException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;

public class ThrowableExternalizer implements AdvancedExternalizer<Throwable> {

   public static ThrowableExternalizer INSTANCE = new ThrowableExternalizer();

   private static final short UNKNOWN = -1;
   // Infinispan Exceptions
   private static final short ALL_OWNERS_LOST = 0;
   private static final short AVAILABILITY = 1;
   private static final short CACHE_CONFIGURATION = 2;
   private static final short CACHE_EXCEPTION = 3;
   private static final short CACHE_LISTENER = 4;
   private static final short CACHE_UNREACHABLE = 5;
   private static final short CACHE_JOIN = 6;
   private static final short CONCURRENT_CHANGE = 7;
   private static final short CONTAINER_FULL = 8;
   private static final short DEADLOCK_DETECTED = 9;
   private static final short EMBEDDED_CACHEMANAGER_STARTUP = 10;
   private static final short ENCODING = 11;
   private static final short INCORRECT_LISTENER = 12;
   private static final short ILLEGAL_LIFECYLE = 13;
   private static final short INVALID_CACHE_USAGE = 14;
   private static final short INVALID_TX = 15;
   private static final short MARSHALLING = 16;
   private static final short NOT_SERIALIZABLE = 17; // Probably not needed, deprecate?
   private static final short OUTDATED_TOPOLOGY = 18;
   private static final short PERSISTENCE = 19;
   private static final short REMOTE = 20;
   private static final short RPC = 21;
   private static final short SUSPECT = 22;
   private static final short TIMEOUT = 23;
   private static final short USER_RAISED_FUNCTIONAL = 24;
   private static final short WRITE_SKEW = 25;
   private final Map<Class<?>, Short> numbers = new HashMap<>(24);

   public ThrowableExternalizer() {
      numbers.put(AllOwnersLostException.class, ALL_OWNERS_LOST);
      numbers.put(AvailabilityException.class, AVAILABILITY);
      numbers.put(CacheConfigurationException.class, CACHE_CONFIGURATION);
      numbers.put(CacheException.class, CACHE_EXCEPTION);
      numbers.put(CacheListenerException.class, CACHE_LISTENER);
      numbers.put(CacheUnreachableException.class, CACHE_UNREACHABLE);
      numbers.put(CacheJoinException.class, CACHE_JOIN);
      numbers.put(ConcurrentChangeException.class, CONCURRENT_CHANGE);
      numbers.put(ContainerFullException.class, CONTAINER_FULL);
      numbers.put(DeadlockDetectedException.class, DEADLOCK_DETECTED);
      numbers.put(EmbeddedCacheManagerStartupException.class, EMBEDDED_CACHEMANAGER_STARTUP);
      numbers.put(EncodingException.class, ENCODING);
      numbers.put(IncorrectListenerException.class, INCORRECT_LISTENER);
      numbers.put(IllegalLifecycleStateException.class, ILLEGAL_LIFECYLE);
      numbers.put(InvalidCacheUsageException.class, INVALID_CACHE_USAGE);
      numbers.put(InvalidTransactionException.class, INVALID_TX);
      numbers.put(MarshallingException.class, MARSHALLING);
      numbers.put(PersistenceException.class, PERSISTENCE);
      numbers.put(RemoteException.class, REMOTE);
      numbers.put(RpcException.class, RPC);
      numbers.put(SuspectException.class, SUSPECT);
      numbers.put(TimeoutException.class, TIMEOUT);
      numbers.put(UserRaisedFunctionalException.class, USER_RAISED_FUNCTIONAL);
      numbers.put(WriteSkewException.class, WRITE_SKEW);
      numbers.put(OutdatedTopologyException.class, OUTDATED_TOPOLOGY);
   }

   @Override
   public Set<Class<? extends Throwable>> getTypeClasses() {
      return Util.asSet(Throwable.class);
   }

   @Override
   public Integer getId() {
      return Ids.EXCEPTIONS;
   }

   @Override
   public void writeObject(ObjectOutput out, Throwable t) throws IOException {
      short id = numbers.getOrDefault(t.getClass(), UNKNOWN);
      out.writeShort(id);

      switch (id) {
         case ALL_OWNERS_LOST:
         case CONCURRENT_CHANGE:
            break;
         case AVAILABILITY:
         case CACHE_CONFIGURATION:
         case CACHE_EXCEPTION:
         case CACHE_LISTENER:
         case CACHE_JOIN:
         case EMBEDDED_CACHEMANAGER_STARTUP:
         case ENCODING:
         case ILLEGAL_LIFECYLE:
         case INVALID_CACHE_USAGE:
         case INVALID_TX:
         case MARSHALLING:
         case PERSISTENCE:
         case REMOTE:
         case RPC:
         case SUSPECT:
            writeMessageAndCause(out, t);
            break;
         case CACHE_UNREACHABLE:
         case CONTAINER_FULL:
         case DEADLOCK_DETECTED:
         case INCORRECT_LISTENER:
         case TIMEOUT:
            MarshallUtil.marshallString(t.getMessage(), out);
            break;
         case OUTDATED_TOPOLOGY:
            OutdatedTopologyException ote = (OutdatedTopologyException) t;
            out.writeBoolean(ote.topologyIdDelta == 0);
            break;
         case USER_RAISED_FUNCTIONAL:
            out.writeObject(t.getCause());
            break;
         case WRITE_SKEW:
            WriteSkewException wse = (WriteSkewException) t;
            writeMessageAndCause(out, wse);
            out.writeObject(wse.getKey());
         default:
            writeGenericThrowable(out, t);
            break;
      }
   }

   @Override
   public Throwable readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      short id = in.readShort();
      String msg;
      Throwable t;
      switch (id) {
         case ALL_OWNERS_LOST:
            return AllOwnersLostException.INSTANCE;
         case AVAILABILITY:
            return new AvailabilityException();
         case CACHE_CONFIGURATION:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new CacheConfigurationException(msg, t);
         case CACHE_EXCEPTION:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new CacheException(msg, t);
         case CACHE_LISTENER:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new CacheListenerException(msg, t);
         case CACHE_JOIN:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new CacheJoinException(msg, t);
         case CACHE_UNREACHABLE:
            msg = MarshallUtil.unmarshallString(in);
            return new CacheUnreachableException(msg);
         case CONCURRENT_CHANGE:
            return new ConcurrentChangeException();
         case CONTAINER_FULL:
            msg = MarshallUtil.unmarshallString(in);
            return new ContainerFullException(msg);
         case DEADLOCK_DETECTED:
            msg = MarshallUtil.unmarshallString(in);
            return new DeadlockDetectedException(msg);
         case EMBEDDED_CACHEMANAGER_STARTUP:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new EmbeddedCacheManagerStartupException(msg, t);
         case ENCODING:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new EncodingException(msg, t);
         case INCORRECT_LISTENER:
            msg = MarshallUtil.unmarshallString(in);
            return new IncorrectListenerException(msg);
         case ILLEGAL_LIFECYLE:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new IllegalLifecycleStateException(msg, t);
         case INVALID_CACHE_USAGE:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new InvalidCacheUsageException(msg, t);
         case INVALID_TX:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new InvalidTransactionException(msg, t);
         case MARSHALLING:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new MarshallingException(msg, t);
         case OUTDATED_TOPOLOGY:
            boolean retryNextTopology = in.readBoolean();
            return retryNextTopology ? OutdatedTopologyException.RETRY_NEXT_TOPOLOGY : OutdatedTopologyException.RETRY_SAME_TOPOLOGY;
         case PERSISTENCE:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new PersistenceException(msg, t);
         case REMOTE:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new RemoteException(msg, t);
         case RPC:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new RpcException(msg, t);
         case SUSPECT:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            return new SuspectException(msg, t);
         case TIMEOUT:
            msg = MarshallUtil.unmarshallString(in);
            return new TimeoutException(msg);
         case USER_RAISED_FUNCTIONAL:
            t = (Throwable) in.readObject();
            return new UserRaisedFunctionalException(t);
         case WRITE_SKEW:
            msg = MarshallUtil.unmarshallString(in);
            t = (Throwable) in.readObject();
            Object key = in.readObject();
            return new WriteSkewException(msg, t, key);
         default:
            return readGenericThrowable(in);
      }
   }

   private void writeMessageAndCause(ObjectOutput out, Throwable t) throws IOException {
      MarshallUtil.marshallString(t.getMessage(), out);
      out.writeObject(t.getCause());
   }

   private void writeGenericThrowable(ObjectOutput out, Throwable t) throws IOException {
      out.writeUTF(t.getClass().getName());
      writeMessageAndCause(out, t);
   }

   private Throwable readGenericThrowable(ObjectInput in) throws IOException, ClassNotFoundException {
      String impl = in.readUTF();
      String msg = MarshallUtil.unmarshallString(in);
      Throwable t = (Throwable) in.readObject();
      try {
         Class<?> clazz = Class.forName(impl);
         if (t == null && msg == null) {
            return (Throwable) clazz.getConstructor().newInstance(new Object[]{});
         } else if (t == null) {
            return (Throwable) clazz.getConstructor(String.class).newInstance(msg);
         } else if (msg == null) {
            return (Throwable) clazz.getConstructor(Throwable.class).newInstance(t);
         }
         return (Throwable) clazz.getConstructor(String.class, Throwable.class).newInstance(msg, t);
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
         throw new MarshallingException(e);
      }
   }
}
