package org.infinispan.marshall.core.internal;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.StreamingMarshaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Java serialization marshaller for unknown types.
 * Unknown types are those types for which there are no externalizers.
 */
final class InternalJavaSerialMarshaller implements StreamingMarshaller {

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      // TODO: Remove temporary check once all types are covered
      // Temporary check to verify that no java.* nor org.infinispan.*
      // instances for which externalizer should have been created get through
      // this method
      String pkg = obj.getClass().getPackage().getName();
      if (obj instanceof Serializable
            && (pkg.startsWith("java.") || pkg.startsWith("org.infinispan.") || pkg.startsWith("org.jgroups."))
            && !isWhiteList(obj.getClass().getName())) {
         throw new RuntimeException("Check support for: " + obj.getClass());
      }

      DelegateOutputStream stream = new DelegateOutputStream(out);
      try (ObjectOutputStream objectStream = new ObjectOutputStream(stream)) {
         objectStream.writeObject(obj);
      }
   }

   private boolean isWhiteList(String className) {
      return className.endsWith("Exception")
            || className.contains("$$Lambda$")
            || className.equals("java.lang.Class")
            || className.equals("java.util.Date") // test
            || className.equals("org.infinispan.api.lazy.LazyCacheAPITest$CustomPojo")
            || className.equals("org.infinispan.atomic.TestDeltaAware")
            || className.equals("org.infinispan.atomic.TestDeltaAware$TestDelta")
            || className.equals("org.infinispan.configuration.override.XMLConfigurationOverridingTest$NonIndexedClass")
            || className.equals("org.infinispan.context.MarshalledValueContextTest$Key")
            || className.equals("org.infinispan.distribution.MagicKey")
            || className.equals("org.infinispan.distribution.SingleOwnerTest$ExceptionExternalizable")
            || className.equals("org.infinispan.distribution.ch.AffinityPartitionerTest$AffinityKey")
            || className.equals("org.infinispan.distribution.groups.BaseUtilGroupTest$GroupKey")
            || className.equals("org.infinispan.distribution.groups.StateTransferGetGroupKeysTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distribution.rehash.NonTxPrimaryOwnerBecomingNonOwnerTest$CustomConsistentHashFactory")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$ExceptionThrowingCallable")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$FailOnlyOnceDistributedCallable")
            || className.equals("org.infinispan.distexec.BasicDistributedExecutorTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DefaultExecutorService$RunnableAdapter")
            || className.equals("org.infinispan.distexec.DistributedExecutionCompletionTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutionCompletionTest$SimpleDistributedCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorBadResponseFailoverTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorTest$LongRunningCallable")
            || className.equals("org.infinispan.distexec.DistributedExecutorFailoverTest$SleepingSimpleCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$ExceptionThrowingCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleCallableWithField")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SimpleDistributedCallable")
            || className.equals("org.infinispan.distexec.LocalDistributedExecutorTest$SleepingSimpleCallable")
            || className.equals("org.infinispan.eviction.impl.EvictionWithConcurrentOperationsTest$SameHashCodeKey")
            || className.equals("org.infinispan.eviction.impl.ManualEvictionWithSizeBasedAndConcurrentOperationsInPrimaryOwnerTest$SameHashCodeKey")
            || className.equals("org.infinispan.eviction.impl.MarshalledValuesManualEvictionTest$ManualEvictionPojo")
            || className.equals("org.infinispan.jmx.RpcManagerMBeanTest$SlowToSerialize")
            || className.equals("org.infinispan.marshall.CustomClass")
            || className.equals("org.infinispan.marshall.InvalidatedMarshalledValueTest$InvalidatedPojo")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child1")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child2")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Human")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$HumanComparator")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Pojo")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling")
            || className.equals("org.infinispan.marshall.core.JBossMarshallingTest$ObjectThatContainsACustomReadObjectMethod")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$Pojo")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$CustomReadObjectMethod")
            || className.equals("org.infinispan.marshall.core.MarshalledValueTest$ObjectThatContainsACustomReadObjectMethod")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$FilterConverter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$LifespanConverter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$LifespanFilter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$NewLifespanLargerFilter")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$StringAppender")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.AbstractClusterListenerUtilTest$StringTruncator")
            || className.equals("org.infinispan.notifications.cachelistener.cluster.NoOpCacheEventFilterConverterWithDependencies")
            || className.equals("org.infinispan.persistence.BaseStoreFunctionalTest$Pojo")
            || className.equals("org.infinispan.persistence.BaseStoreTest$Pojo")
            || className.equals("org.infinispan.remoting.TransportSenderExceptionHandlingTest$FailureType")
            || className.equals("org.infinispan.replication.ReplicationExceptionTest$ContainerData")
            || className.equals("org.infinispan.statetransfer.BigObject")
            || className.equals("org.infinispan.statetransfer.ReadAfterLosingOwnershipTest$SingleKeyConsistentHashFactory")
            || className.equals("org.infinispan.statetransfer.RemoteGetDuringStateTransferTest$SingleKeyConsistentHashFactory")
            || className.equals("org.infinispan.statetransfer.StateTransferCacheLoaderFunctionalTest$DelayedUnmarshal")
            || className.equals("org.infinispan.statetransfer.StateTransferFunctionalTest$DelayTransfer")
            || className.equals("org.infinispan.statetransfer.WriteSkewDuringStateTransferTest$ConsistentHashFactoryImpl")
            || className.equals("org.infinispan.stats.impl.ClusterCacheStatsImpl$DistributedCacheStatsCallable") // prod
            || className.equals("org.infinispan.stream.BaseSetupStreamIteratorTest$StringTruncator")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachDoubleInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachIntInjected")
            || className.equals("org.infinispan.stream.BaseStreamTest$ForEachLongInjected")
            || className.equals("org.infinispan.stream.DistributedStreamIteratorWithStoreAsBinaryTest$MagicKeyStringFilter")
            || className.equals("org.infinispan.test.data.Key")
            || className.equals("org.infinispan.test.data.Person")
            || className.equals("org.infinispan.xsite.BackupSender$TakeSiteOfflineResponse") // prod
            || className.equals("org.infinispan.xsite.BackupSender$BringSiteOnlineResponse") // prod
            || className.equals("org.infinispan.xsite.XSiteAdminCommand$Status") // prod

            || className.equals("org.infinispan.persistence.jdbc.TableNameUniquenessTest$Person")
            || className.equals("org.infinispan.persistence.jdbc.binary.JdbcBinaryStoreTest$FixedHashKey")
            || className.equals("org.infinispan.persistence.jdbc.stringbased.Person")
            ;
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      DelegateInputStream stream = new DelegateInputStream(in);
      try (ObjectInputStream objectStream = new ObjectInputStream(stream)) {
         return objectStream.readObject();
      }
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new RuntimeException("NYI");
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void stop() {
      throw new RuntimeException("NYI");
   }

   @Override
   public void start() {
      throw new RuntimeException("NYI");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      throw new RuntimeException("NYI");
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      throw new RuntimeException("NYI");
   }

   final static class DelegateOutputStream extends OutputStream {

      final ObjectOutput out;

      DelegateOutputStream(ObjectOutput out) {
         this.out = out;
      }

      @Override
      public void write(int b) throws IOException {
         out.writeByte(b);
      }

   }

   final static class DelegateInputStream extends InputStream {

      final ObjectInput in;

      DelegateInputStream(ObjectInput in) {
         this.in = in;
      }

      @Override
      public int read() throws IOException {
         return in.readUnsignedByte();
      }

   }

}
