package org.infinispan.client.rest.impl.jdk.form;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Flow;

import org.infinispan.commons.dataconversion.MediaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the multipart/form-data body produced by {@link MultiPartRestEntityJDK}.
 */
class MultiPartRestEntityTest {

   @TempDir
   Path tempDir;

   @Test
   void contentTypeIsMultipartForm() {
      MultiPartRestEntityJDK entity = new MultiPartRestEntityJDK();
      entity.addPart("part", "value");
      assertThat(entity.contentType().toString()).startsWith("multipart/form-data; boundary=");
   }

   @Test
   void emptyBodyPublisherThrows() {
      MultiPartRestEntityJDK entity = new MultiPartRestEntityJDK();
      assertThatThrownBy(entity::bodyPublisher).isInstanceOf(IllegalStateException.class);
   }

   @Test
   void stringPartProducesMultipartBody() {
      MultiPartRestEntityJDK entity = new MultiPartRestEntityJDK();
      entity.addPart("name", "John");
      String body = new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8);
      String boundary = extractBoundary(entity);
      assertThat(body).startsWith("--" + boundary + "\r\n");
      assertThat(body).contains("Content-Disposition: form-data; name=name");
      assertThat(body).contains("Content-Type: text/plain; charset=UTF-8");
      assertThat(body).contains("\r\n\r\nJohn\r\n");
      assertThat(body).endsWith("--" + boundary + "--");
   }

   @Test
   void streamPartProducesMultipartBody() {
      MultiPartRestEntityJDK entity = new MultiPartRestEntityJDK();
      entity.addPart("file", () -> new ByteArrayInputStream("file-content".getBytes(StandardCharsets.UTF_8)), "file.txt", "text/plain");
      String body = new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8);
      String boundary = extractBoundary(entity);
      assertThat(body).contains("Content-Disposition: form-data; name=file; filename=file.txt");
      assertThat(body).contains("Content-Type: text/plain");
      assertThat(body).contains("file-content");
      assertThat(body).endsWith("--" + boundary + "--");
   }

   @Test
   void filePartProducesMultipartBody() throws Exception {
      Path file = tempDir.resolve("upload.txt");
      Files.writeString(file, "file-content");
      MultiPartRestEntityJDK entity = new MultiPartRestEntityJDK();
      entity.addPart("file", file, MediaType.TEXT_PLAIN);
      String body = new String(publishBody(entity.bodyPublisher()), StandardCharsets.UTF_8);
      String boundary = extractBoundary(entity);
      assertThat(body).contains("Content-Disposition: form-data; name=file; filename=upload.txt");
      assertThat(body).contains("file-content");
      assertThat(body).endsWith("--" + boundary + "--");
   }

   /**
    * Extracts the {@code boundary} parameter from the multipart content type.
    */
   private static String extractBoundary(MultiPartRestEntityJDK entity) {
      String contentType = entity.contentType().toString();
      int idx = contentType.indexOf("boundary=");
      assertThat(idx).isNotNegative();
      return contentType.substring(idx + "boundary=".length()).trim();
   }

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
