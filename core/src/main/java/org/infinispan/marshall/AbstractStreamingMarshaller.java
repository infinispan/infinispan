package org.infinispan.marshall;

import org.infinispan.io.ExposedByteArrayOutputStream;

import java.io.*;

/**
 * Abstract marshaller
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractStreamingMarshaller extends AbstractMarshaller implements StreamingMarshaller {

   @Override
   public Object objectFromInputStream(InputStream inputStream) throws IOException, ClassNotFoundException {
      // TODO: available() call commented until https://issues.apache.org/jira/browse/HTTPCORE-199 httpcore-nio issue is fixed. 
      // int len = inputStream.available();
      ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(DEFAULT_BUF_SIZE);
      byte[] buf = new byte[Math.min(DEFAULT_BUF_SIZE, 1024)];
      int bytesRead;
      while ((bytesRead = inputStream.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

}
