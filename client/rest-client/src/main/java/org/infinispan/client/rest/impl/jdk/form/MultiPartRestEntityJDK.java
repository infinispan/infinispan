package org.infinispan.client.rest.impl.jdk.form;

import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_DISPOSITION;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_LENGTH;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_TRANSFER_ENCODING;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Supplier;

import org.infinispan.client.rest.MultiPartRestEntity;
import org.infinispan.commons.dataconversion.MediaType;

public class MultiPartRestEntityJDK implements MultiPartRestEntity {
   public static final String CRLF = "\r\n";
   public static final String DOUBLE_DASH = "--";
   private final List<PartsSpecification> partsSpecificationList = new ArrayList<>();
   private final String boundary = UUID.randomUUID().toString();

   @Override
   public MultiPartRestEntityJDK addPart(String name, String value) {
      PartsSpecification newPart = new PartsSpecification();
      newPart.type = PartsSpecification.TYPE.STRING;
      newPart.name = name;
      newPart.value = value;
      partsSpecificationList.add(newPart);
      return this;
   }

   @Override
   public MultiPartRestEntityJDK addPart(String name, Path value, MediaType contentType) {
      PartsSpecification newPart = new PartsSpecification();
      newPart.type = PartsSpecification.TYPE.FILE;
      newPart.name = name;
      newPart.path = value;
      partsSpecificationList.add(newPart);
      return this;
   }

   public MultiPartRestEntityJDK addPart(String name, Supplier<InputStream> value, String filename, String contentType) {
      PartsSpecification newPart = new PartsSpecification();
      newPart.type = PartsSpecification.TYPE.STREAM;
      newPart.name = name;
      newPart.stream = value;
      newPart.filename = filename;
      newPart.contentType = contentType;
      partsSpecificationList.add(newPart);
      return this;
   }

   private void addFinalBoundaryPart() {
      PartsSpecification newPart = new PartsSpecification();
      newPart.type = PartsSpecification.TYPE.FINAL_BOUNDARY;
      newPart.value = DOUBLE_DASH + boundary + DOUBLE_DASH;
      partsSpecificationList.add(newPart);
   }

   @Override
   public HttpRequest.BodyPublisher bodyPublisher() {
      if (partsSpecificationList.isEmpty()) {
         throw new IllegalStateException("Must have at least one part to build multipart message.");
      }
      addFinalBoundaryPart();
      return HttpRequest.BodyPublishers.ofByteArrays(PartsIterator::new);
   }

   @Override
   public MediaType contentType() {
      return MediaType.fromString("multipart/form-data; boundary=" + boundary);
   }

   static class PartsSpecification {
      public enum TYPE {
         STRING, FILE, STREAM, FINAL_BOUNDARY
      }

      PartsSpecification.TYPE type;
      String name;
      String value;
      Path path;
      Supplier<InputStream> stream;
      String filename;
      String contentType;

   }

   class PartsIterator implements Iterator<byte[]> {

      private final Iterator<PartsSpecification> it;
      private InputStream currentFileInput;

      private boolean done;
      private byte[] next;

      PartsIterator() {
         it = partsSpecificationList.iterator();
      }

      @Override
      public boolean hasNext() {
         if (done) return false;
         if (next != null) return true;
         try {
            next = computeNext();
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }
         if (next == null) {
            done = true;
            return false;
         }
         return true;
      }

      @Override
      public byte[] next() {
         if (!hasNext()) throw new NoSuchElementException();
         byte[] res = next;
         next = null;
         return res;
      }

      private byte[] computeNext() throws IOException {
         if (currentFileInput == null) {
            if (!it.hasNext()) return null;
            PartsSpecification nextPart = it.next();
            if (PartsSpecification.TYPE.STRING.equals(nextPart.type)) {
               String part =
                     DOUBLE_DASH + boundary + CRLF +
                           CONTENT_DISPOSITION + ": form-data; name=" + nextPart.name + CRLF +
                           CONTENT_TYPE + ": text/plain; charset=UTF-8" + CRLF + CRLF +
                           nextPart.value + CRLF;
               return part.getBytes(StandardCharsets.UTF_8);
            }
            if (PartsSpecification.TYPE.FINAL_BOUNDARY.equals(nextPart.type)) {
               return nextPart.value.getBytes(StandardCharsets.UTF_8);
            }
            String filename;
            String contentType;
            long size;
            if (PartsSpecification.TYPE.FILE.equals(nextPart.type)) {
               Path path = nextPart.path;
               filename = path.getFileName().toString();
               size = Files.size(path);
               contentType = Files.probeContentType(path);
               if (contentType == null) contentType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
               currentFileInput = Files.newInputStream(path);
            } else {
               filename = nextPart.filename;
               size = 0;
               contentType = nextPart.contentType;
               if (contentType == null) contentType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
               currentFileInput = nextPart.stream.get();
            }
            String encoding = MediaType.TEXT_PLAIN_TYPE.equals(contentType) ? "8bit" : "binary";
            String partHeader =
                  DOUBLE_DASH + boundary + CRLF +
                        CONTENT_DISPOSITION + ": form-data; name=" + nextPart.name + "; filename=" + filename + CRLF +
                        CONTENT_TYPE + ": " + contentType + CRLF +
                        CONTENT_TRANSFER_ENCODING + ": " + encoding + CRLF +
                        (size == 0 ? "" : CONTENT_LENGTH + ": " + size + CRLF ) +
                        CRLF;
            return partHeader.getBytes(StandardCharsets.UTF_8);
         } else {
            byte[] buf = new byte[8192];
            int r = currentFileInput.read(buf);
            if (r > 0) {
               byte[] actualBytes = new byte[r];
               System.arraycopy(buf, 0, actualBytes, 0, r);
               return actualBytes;
            } else {
               currentFileInput.close();
               currentFileInput = null;
               return CRLF.getBytes(StandardCharsets.UTF_8);
            }
         }
      }
   }
}
