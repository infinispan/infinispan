package org.infinispan.xsite.events;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRemoteEventCommand;

/**
 * A collector of events to be sent to the remote site.
 * <p>
 * This class implements {@link AutoCloseable} so it can be used with try-with-resources. The {@link #close()} methods sends the events.
 *
 * @since 15.0
 */
public class XSiteEventSender implements AutoCloseable {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final Transport transport;
   private final Map<ByteString, EventList> eventsToRemoteSites;

   public XSiteEventSender(Transport transport) {
      eventsToRemoteSites = new ConcurrentHashMap<>(4);
      this.transport = transport;
   }

   public void addEventToSite(ByteString site, XSiteEvent event) {
      log.debugf("Added event %s to %s", event, site);
      eventsToRemoteSites.computeIfAbsent(site, EventList::new).add(event);
   }

   @Override
   public void close() throws Exception {
      log.debug("Flushing events");
      for (var eventList : eventsToRemoteSites.values()) {
         var cmd = new XSiteRemoteEventCommand(eventList.events);
         var backup = new XSiteBackup(eventList.site.toString(), false, 10000);
         transport.backupRemotely(backup, cmd);
      }
   }


   private static class EventList {
      final ByteString site;
      final List<XSiteEvent> events;

      EventList(ByteString site) {
         this.site = site;
         events = Collections.synchronizedList(new LinkedList<>());
      }

      void add(XSiteEvent event) {
         events.add(event);
      }
   }
}
