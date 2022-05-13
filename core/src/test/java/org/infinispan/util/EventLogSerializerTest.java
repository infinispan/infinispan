package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;

import java.io.StringWriter;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogSerializer;
import org.testng.annotations.Test;

@Test(testName = "util.EventLogSerializerTest", groups = "unit")
public class EventLogSerializerTest extends AbstractInfinispanTest {
   private static final String JSON_TEMPLATE = "{\n  \"log\" : {\n"
         + "    \"category\" : \"CLUSTER\",\n    \"content\" : {\n      \"level\" : \"INFO\",\n"
         + "      \"message\" : \"%s\",\n      \"detail\" : \"%s\"\n    },\n    \"meta\" : {\n"
         + "      \"instant\" : \"%s\",\n      \"context\" : \"%s\",\n      \"scope\" : null,\n"
         + "      \"who\" : null\n    }\n  }\n}";
   private static final String XML_TEMPLATE = "<?xml version=\"1.0\"?>\n"
         + "<log category=\"CLUSTER\">\n"
         + "    <content level=\"INFO\" message=\"%s\" detail=\"%s\"/>\n"
         + "    <meta instant=\"%s\" context=\"%s\" scope=\"\" who=\"\"/>\n"
         + "</log>\n";
   private static final String YAML_TEMPLATE = "log: \n  category: \"CLUSTER\"\n"
         + "  content: \n    level: \"INFO\"\n    message: \"%s\"\n    detail: \"%s\"\n"
         + "  meta: \n    instant: \"%s\"\n    context: \"%s\"\n    scope: ~\n"
         + "    who: ~\n";

   private final EventLogSerializer serializer = new EventLogSerializer();

   public void testJsonSerialization() {
      EventLog log = new TestEventLog();
      String expected = String.format(JSON_TEMPLATE, log.getMessage(), log.getDetail().get(), log.getWhen(), log.getContext().get());
      String actual = serialize(log, MediaType.APPLICATION_JSON);
      assertEquals(expected, actual);
   }

   public void testXmlSerialization() {
      EventLog log = new TestEventLog();
      String expected = String.format(XML_TEMPLATE, log.getMessage(), log.getDetail().get(), log.getWhen(), log.getContext().get());
      String actual = serialize(log, MediaType.APPLICATION_XML);
      assertEquals(expected, actual);
   }

   public void testYamlSerialization() {
      EventLog log = new TestEventLog();
      String expected = String.format(YAML_TEMPLATE, log.getMessage(), log.getDetail().get(), log.getWhen(), log.getContext().get());
      String actual = serialize(log, MediaType.APPLICATION_YAML);
      assertEquals(expected, actual);
   }

   private String serialize(EventLog log, MediaType type) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter cw = ConfigurationWriter.to(sw).withType(type).build()) {
         cw.writeStartDocument();
         serializer.serialize(cw, log);
         cw.writeEndDocument();
      }

      return sw.toString();
   }

   private static class TestEventLog implements EventLog {
      private final Instant now = Instant.now();
      private final String message = UUID.randomUUID().toString();
      private final String detail = UUID.randomUUID().toString();
      private final String context = UUID.randomUUID().toString();

      @Override
      public Instant getWhen() {
         return now;
      }

      @Override
      public EventLogLevel getLevel() {
         return EventLogLevel.INFO;
      }

      @Override
      public String getMessage() {
         return message;
      }

      @Override
      public EventLogCategory getCategory() {
         return EventLogCategory.CLUSTER;
      }

      @Override
      public Optional<String> getDetail() {
         return Optional.of(detail);
      }

      @Override
      public Optional<String> getWho() {
         return Optional.empty();
      }

      @Override
      public Optional<String> getContext() {
         return Optional.of(context);
      }

      @Override
      public Optional<String> getScope() {
         return Optional.empty();
      }

      @Override
      public int compareTo(EventLog eventLog) {
         return eventLog.getWhen().compareTo(getWhen());
      }
   }
}
