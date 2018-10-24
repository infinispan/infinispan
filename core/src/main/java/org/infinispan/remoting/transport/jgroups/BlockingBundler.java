package org.infinispan.remoting.transport.jgroups;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.jcip.annotations.GuardedBy;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.protocols.Bundler;
import org.jgroups.protocols.TP;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

/**
 * A {@link Bundler} implementation that blocks application threads as soon as it has more than one bundle's
 * worth of messages waiting to be sent.
 *
 * @author Dan Berindei
 * @since 9.4
 */
public class BlockingBundler implements Bundler, Runnable {
   private static final String THREAD_NAME = "BlockingBundler";

   private final Lock bundlerLock = new ReentrantLock();
   private final Condition bundlerCondition = bundlerLock.newCondition();

   private TP transport;
   private Log log;
   private short[] excluded_headers;

   private Destination multicasts;
   private Map<Address, Destination> unicasts;
   private volatile boolean blockBundlerThread;

   private Thread bundlerThread;
   private volatile boolean running;

   @Override
   public void init(TP transport) {
      this.transport = transport;

      log = transport.getLog();
      // exclude the transport header
      excluded_headers = new short[]{transport.getId()};

      multicasts = new Destination(null);
      unicasts = new ConcurrentHashMap<>();
   }

   @Override
   public void start() {
      bundlerThread = transport.getThreadFactory().newThread(this, THREAD_NAME);
      running = true;
      bundlerThread.start();
   }

   @Override
   public void stop() {
      running = false;
      bundlerThread.interrupt();
      try {
         bundlerThread.join(500);
      } catch (InterruptedException e) {
         // Ignore it, we're already stopping
         Thread.currentThread().interrupt();
      }
      bundlerThread = null;

      unicasts.clear();
   }

   @Override
   public void send(Message msg) throws Exception {
      Address dest = msg.dest();
      if (dest == null) {
         multicasts.addMessage(msg);
      } else {
         unicasts.computeIfAbsent(dest, Destination::new).addMessage(msg);
      }
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public Map<String, Object> getStats() {
      return null;
   }

   @Override
   public void resetStats() {

   }

   @Override
   public void viewChange(View view) {
      // TODO Remove destinations not in the view, ideally without discarding messages
   }

   @Override
   public void run() {
      while (running) {
         boolean sent = sendQueued();
         if (sent) {
            // Run at least one more iteration before blocking
            blockBundlerThread = false;
         } else {
            bundlerLock.lock();
            try {
               if (!blockBundlerThread) {
                  // We didn't send any message, block after the next iteration
                  blockBundlerThread = true;
               } else {
                  // Previous 2 iterations didn't send any message, block now
                  while (blockBundlerThread) {
                     bundlerCondition.await();
                  }
               }
            } catch (InterruptedException e) {
               assert !running;
            } finally {
               bundlerLock.unlock();
            }
         }
      }
   }

   boolean sendQueued() {
      boolean sent = multicasts.trySendBundle();
      for (Map.Entry<Address, Destination> entry : unicasts.entrySet()) {
         Destination v = entry.getValue();
         sent |= v.trySendBundle();
      }
      return sent;
   }

   void writeBundleHeader(DataOutput output, Address dest, Address src, int numMsgs) throws Exception {
      Util.writeMessageListHeader(dest, src, transport.getClusterNameAscii().chars(), numMsgs, output, dest == null);
   }

   void writeMessages(ByteArrayDataOutputStream output, final Address src, final Collection<Message> messages)
      throws Exception {
      for (Message msg : messages) {
         msg.writeToNoAddrs(transport.localAddress(), output, excluded_headers);
      }
   }

   void sendBundle(ByteArrayDataOutputStream output, final Address dest) throws Exception {
      log.trace("Sending message/bundle of %d bytes", output.position());
      transport.doSend(output.buffer(), 0, output.position(), dest);
   }


   private void signalBundlerThread() {
      if (!blockBundlerThread)
         return;

      bundlerLock.lock();
      try {
         blockBundlerThread = false;
         bundlerCondition.signalAll();
      } finally {
         bundlerLock.unlock();
      }
   }

   /**
    * The application thread writes to currentBundle.
    * If currentBundle is full, it tries to serialize the bundle.
    * <p>
    * The bundler thread performs the actual send.
    */
   private class Destination {
      final Lock destinationLock = new ReentrantLock();
      final Condition addCondition = destinationLock.newCondition();
      final Address destination;
      int currentBundleSize;
      List<Message> currentBundle;
      ByteArrayDataOutputStream serializationBuffer;
      volatile boolean currentBundleEmpty = true;
      volatile boolean bufferEmpty = true;

      Destination(Address destination) {
         this.destination = destination;

         currentBundle = new ArrayList<>(100);
         serializationBuffer = new ByteArrayDataOutputStream(transport.getMaxBundleSize() + MSG_OVERHEAD);
      }

      void addMessage(Message message) throws Exception {
         destinationLock.lock();
         try {
            long size = message.size();
            int maxBundleSize = transport.getMaxBundleSize();
            assert size < maxBundleSize;

            // Wait for space in the current bundle or for the serialization buffer to be free
            // TODO This penalizes threads sending large messages, because they wake up and go back to the end of the queue
            // We should implement a wait queue that can skip large messages when writing a bundle, but preserves
            // their priority for the next bundle
            while (!bufferEmpty && currentBundleSize + size > maxBundleSize) {
               // TODO Add a time limit and drop messages if waiting too long
               addCondition.await();
            }

            if (currentBundleSize + size > maxBundleSize) {
               // The current bundle is full, but the serialization buffer is free
               writeCurrentBundle();
               // Now the current bundle is empty and the serialization buffer is used
            }

            currentBundle.add(message);
            currentBundleSize += size;
            if (currentBundleEmpty) {
               currentBundleEmpty = false;
            }
         } finally {
            destinationLock.unlock();
         }

         signalBundlerThread();
      }

      boolean trySendBundle() {
         if (currentBundleEmpty)
            return false;

         try {
            if (bufferEmpty) {
               destinationLock.lock();
               try {
                  // No other thread can empty the buffer or the current bundle
                  assert bufferEmpty;
                  assert !currentBundleEmpty;

                  // The bundle isn't full, so we have to serialize it on the bundler thread
                  // TODO Serialize the messages when they are added, and only serialize the header here
                  writeCurrentBundle();

                  // Application threads could wait for room in the current bundle only if it's not empty
                  if (!currentBundleEmpty) {
                     addCondition.signalAll();
                  }
               } finally {
                  destinationLock.unlock();
               }
            }

            sendBundle(serializationBuffer, destination);

            destinationLock.lock();
            try {
               serializationBuffer.position(0);
               bufferEmpty = true;

               // Even if we didn't serialize the current bundle, we made room for an application thread
               // to serialize the current bundle itself and add its message
               if (!currentBundleEmpty) {
                  addCondition.signalAll();
               }
            } finally {
               destinationLock.unlock();
            }
         } catch (Throwable t) {
            log.error(Util.getMessage("FailureSendingMsgBundle"), transport.localAddress(), t);
         }
         return true;
      }

      @GuardedBy("destinationLock")
      private void writeCurrentBundle() throws Exception {
         assert serializationBuffer.position() == 0;
         assert currentBundle.size() > 0;
         if (currentBundle.size() == 1) {
            Util.writeMessage(currentBundle.get(0), serializationBuffer, destination == null);
         } else {
            writeBundleHeader(serializationBuffer, destination, transport.localAddress(),
                              currentBundle.size());
            writeMessages(serializationBuffer, transport.localAddress(), currentBundle);
         }
         if (log.isTraceEnabled()) {
            log.trace("Serialized %d message(s) in %d bytes", currentBundle.size(), serializationBuffer.position());
         }
         currentBundle.clear();
         currentBundleSize = 0;
         currentBundleEmpty = true;
         bufferEmpty = false;
      }
   }
}
