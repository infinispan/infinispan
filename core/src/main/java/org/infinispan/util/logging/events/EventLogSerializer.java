package org.infinispan.util.logging.events;

import java.util.Optional;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * @since 14.0
 */
public class EventLogSerializer implements ConfigurationSerializer<EventLog> {

   @Override
   public void serialize(ConfigurationWriter writer, EventLog event) {
      writer.writeStartElement("log");
      writer.writeAttribute("category", event.getCategory().name());

      writer.writeStartElement("content");
      writer.writeAttribute("level", event.getLevel().name());
      writer.writeAttribute("message", event.getMessage());
      writer.writeAttribute("detail", unwrap(event.getDetail()));
      writer.writeEndElement();

      writer.writeStartElement("meta");
      writer.writeAttribute("instant", event.getWhen().toString());
      writer.writeAttribute("context", unwrap(event.getContext()));
      writer.writeAttribute("scope", unwrap(event.getScope()));
      writer.writeAttribute("who", unwrap(event.getWho()));
      // `meta` object
      writer.writeEndElement();

      // `log` object
      writer.writeEndElement();
   }

   private String unwrap(Optional<String> optional) {
      return optional.orElse(null);
   }
}
