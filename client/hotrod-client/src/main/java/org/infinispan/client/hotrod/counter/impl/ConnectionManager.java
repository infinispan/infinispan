package org.infinispan.client.hotrod.counter.impl;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.counter.operation.AddListenerOperation;
import org.infinispan.client.hotrod.counter.operation.RemoveListenerOperation;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Util;

import net.jcip.annotations.GuardedBy;

/**
 * Handles the connection to the Hot Rod server and reads the events.
 * <p>
 * The events are dispatched to the consumer.
 * <p>
 * A random {@code listener-id} is created for this instance.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class ConnectionManager {

   private static final Log log = LogFactory.getLog(ConnectionManager.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();

   private final byte[] listenerId;
   private final CounterOperationFactory factory;
   private final Consumer<HotRodCounterEvent> eventConsumer;
   private final Codec codec;
   private final ExecutorService executor;
   @GuardedBy("this")
   private final Set<String> registeredCounters;
   @GuardedBy("this")
   private EventDispatcher dispatcher;

   ConnectionManager(CounterOperationFactory factory, Consumer<HotRodCounterEvent> eventConsumer) {
      this.factory = factory;
      this.codec = factory.getCodec();
      this.eventConsumer = eventConsumer;
      listenerId = new byte[16];
      ThreadLocalRandom.current().nextBytes(listenerId);
      //TODO? a shared executor between client listener notifier and the connection manager...
      executor = Executors.newCachedThreadPool(ClientListenerNotifier.getRestoreThreadNameThreadFactory());
      registeredCounters = new HashSet<>();
   }

   private static void debugMessage(String message) {
      if (debug) {
         log.debug(message);
      }
   }

   public synchronized void failedServers(Collection<SocketAddress> failedServers) {
      if (failedServers.contains(getServerInUse())) {
         findAnotherServer();
      }
   }

   public synchronized void stop() {
      removeConnection("");
      cleanupDispatcher();
      executor.shutdownNow();
   }

   synchronized void addConnection(String counterName) {
      createConnection(counterName);
      registeredCounters.add(counterName);
   }

   synchronized void removeConnection(String counterName) {
      if (trace) {
         log.tracef("Remove listener connection for counter '%s'", counterName);
      }
      RemoveListenerOperation op = factory.newRemoveListenerOperation(counterName, listenerId, getServerInUse());
      if (op.execute()) {
         //server no longer uses this connection
         cleanupDispatcher();
      }
      registeredCounters.remove(counterName);
   }

   private void createConnection(String counterName) {
      AddListenerOperation op = factory.newAddListenerOperation(counterName, listenerId, getServerInUse());
      if (op.execute()) {
         //we have a new connection.
         cleanupDispatcher();
         dispatcher = new EventDispatcher(op.getDedicatedTransport());
         if (trace) {
            log.tracef("Add listener connection for counter '%s'. Server used=%s", counterName, getServerInUse());
         }
         executor.execute(dispatcher);
      }
   }

   private SocketAddress getServerInUse() {
      return dispatcher == null ?
             null :
             dispatcher.transport.getRemoteSocketAddress();
   }

   private void cleanupDispatcher() {
      if (dispatcher != null) {
         dispatcher.run = false;
         dispatcher.transport.release(); //disconnect
         dispatcher = null;
      }
   }

   private String getThreadName() {
      return "Counter-Listener-" + Util.toHexString(listenerId, 8);
   }

   private synchronized void findAnotherServer() {
      cleanupDispatcher();
      try {
         if (debug) {
            log.debug("Connection reset by peer, finding another server for counter listeners");
         }
         registeredCounters.forEach(this::createConnection);
      } catch (TransportException e) {
         if (debug) {
            log.debug("Unable to find another server, so ignore connection reset");
         }
         try {
            factory.geTransportFactory().addDisconnectedListener(this::findAnotherServer);
         } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
         }
      }
   }

   private class EventDispatcher implements Runnable {


      private final Transport transport;
      public volatile boolean run = true;

      private EventDispatcher(Transport transport) {
         this.transport = transport;
      }

      @Override
      public void run() {
         Thread.currentThread().setName(getThreadName());
         while (run && !Thread.currentThread().isInterrupted()) {
            HotRodCounterEvent event = null;
            try {
               event = codec.readCounterEvent(transport, listenerId); //read event
               eventConsumer.accept(event);
               // Nullify event, makes it easier to identify network vs invocation error messages
               event = null;
            } catch (TransportException e) {
               handleTransportException(e, event);
            } catch (CancelledKeyException e) {
               // Cancelled key exceptions are also thrown when the channel has been closed
               debugMessage("Key cancelled, most likely channel closed, exiting event reader thread");
               run = false;
            } catch (Throwable t) {
               handleException(t, event);
            }
         }
         if (trace) {
            log.trace("Dispatcher thread stopped!");
         }
      }

      private void handleException(Throwable t, HotRodCounterEvent event) {
         if (event != null) {
            log.unexpectedErrorConsumingEvent(event, t);
         } else {
            log.unableToReadEventFromServer(t, transport.getRemoteSocketAddress());
         }
         if (!transport.isValid()) {
            run = false;
         }
      }

      private void handleTransportException(TransportException e, HotRodCounterEvent event) {
         Throwable cause = e.getCause();
         if (cause instanceof ClosedChannelException || (cause instanceof SocketException && !transport.isValid())) {
            // Channel closed, ignore and exit
            debugMessage("Channel closed, exiting event reader thread");
            run = false;
         } else if (cause instanceof SocketTimeoutException) {
            debugMessage("Timed out reading event, retry");
         } else if (event != null) {
            log.unexpectedErrorConsumingEvent(event, e);
         } else if (cause instanceof IOException && cause.getMessage().contains("Connection reset by peer")) {
            findAnotherServer();
            run = false;
         } else {
            // Server is likely gone!
            log.unrecoverableErrorReadingEvent(e, transport.getRemoteSocketAddress());
            run = false;
         }
      }
   }
}
