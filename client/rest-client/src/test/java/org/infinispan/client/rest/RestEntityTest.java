package org.infinispan.client.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.infinispan.commons.dataconversion.MediaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the {@link RestEntity} factory methods, which are pure logic and independent of any running server.
 */
class RestEntityTest {

   @TempDir
   Path tempDir;

   @Test
   void createStringInfersJson() {
      assertThat(RestEntity.create("{\"a\":1}").contentType()).isEqualTo(MediaType.APPLICATION_JSON);
   }

   @Test
   void createStringInfersXml() {
      assertThat(RestEntity.create("<a/>").contentType()).isEqualTo(MediaType.APPLICATION_XML);
   }

   @Test
   void createStringDefaultsToText() {
      assertThat(RestEntity.create("plain text").contentType()).isEqualTo(MediaType.TEXT_PLAIN);
   }

   @Test
   void createStringPublishesBody() {
      RestEntity entity = RestEntity.create("{\"a\":1}");
      assertThat(new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8)).isEqualTo("{\"a\":1}");
   }

   @Test
   void createWithTypeAndString() {
      RestEntity entity = RestEntity.create(MediaType.APPLICATION_JSON, "body");
      assertThat(entity.contentType()).isEqualTo(MediaType.APPLICATION_JSON);
      assertThat(new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8)).isEqualTo("body");
   }

   @Test
   void createWithTypeAndByteArray() {
      RestEntity entity = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "body".getBytes(StandardCharsets.UTF_8));
      assertThat(entity.contentType()).isEqualTo(MediaType.APPLICATION_OCTET_STREAM);
      assertThat(publishBody(entity.bodyPublisher())).isEqualTo("body".getBytes(StandardCharsets.UTF_8));
   }

   @Test
   void createFileInfersJsonFromContent() throws Exception {
      Path file = tempDir.resolve("data.dat");
      Files.writeString(file, "{\"a\":1}");
      assertThat(RestEntity.create(file.toFile()).contentType()).isEqualTo(MediaType.APPLICATION_JSON);
   }

   @Test
   void createFileInfersXmlFromContent() throws Exception {
      Path file = tempDir.resolve("data.dat");
      Files.writeString(file, "<a/>");
      assertThat(RestEntity.create(file.toFile()).contentType()).isEqualTo(MediaType.APPLICATION_XML);
   }

   @Test
   void createFileYamlByExtension() throws Exception {
      Path file = tempDir.resolve("data.yaml");
      Files.writeString(file, "a: 1");
      assertThat(RestEntity.create(file.toFile()).contentType()).isEqualTo(MediaType.APPLICATION_YAML);
   }

   @Test
   void createFileDefaultsToOctetStream() throws Exception {
      Path file = tempDir.resolve("data.bin");
      Files.writeString(file, "no recognizable marker");
      assertThat(RestEntity.create(file.toFile()).contentType()).isEqualTo(MediaType.APPLICATION_OCTET_STREAM);
   }

   @Test
   void formUrlEncodes() {
      RestEntity entity = RestEntity.form(Map.of("a", List.of("b c", "d")));
      assertThat(entity.contentType()).isEqualTo(MediaType.APPLICATION_WWW_FORM_URLENCODED);
      assertThat(new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8)).isEqualTo("a=b+c&a=d");
   }

   @Test
   void emptyHasNoBodyAndNoContentType() {
      RestEntity entity = RestEntity.empty();
      assertThat(entity.contentType()).isNull();
      assertThat(publishBody(entity.bodyPublisher())).isEmpty();
   }

   /**
    * Drains a {@link HttpRequest.BodyPublisher} into a single byte array.
    */
   private static byte[] publishBody(HttpRequest.BodyPublisher publisher) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      publisher.subscribe(new Flow.Subscriber<>() {
         private Flow.Subscription subscription;

         @Override
         public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            s.request(1);
         }

         @Override
         public void onNext(ByteBuffer b) {
            byte[] arr = new byte[b.remaining()];
            b.get(arr);
            out.write(arr, 0, arr.length);
            subscription.request(1);
         }

         @Override
         public void onError(Throwable t) {
            throw new RuntimeException(t);
         }

         @Override
         public void onComplete() {
         }
      });
      return out.toByteArray();
   }
}
