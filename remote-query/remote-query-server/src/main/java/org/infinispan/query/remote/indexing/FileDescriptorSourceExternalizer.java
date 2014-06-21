package org.infinispan.query.remote.indexing;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.FileDescriptorSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Marshaller for FileDescriptorSource
 *
 * @author gustavonalle
 * @since 7.0
 */
public class FileDescriptorSourceExternalizer extends AbstractExternalizer<FileDescriptorSource> {


   @Override
   @SuppressWarnings("unchecked")
   public Set<Class<? extends FileDescriptorSource>> getTypeClasses() {
      return Util.<Class<? extends FileDescriptorSource>>asSet(FileDescriptorSource.class);
   }

   @Override
   public void writeObject(ObjectOutput output, FileDescriptorSource object) throws IOException {
      Map<String, char[]> fileDescriptors = object.getFileDescriptors();
      Set<Map.Entry<String, char[]>> entries = fileDescriptors.entrySet();
      UnsignedNumeric.writeUnsignedInt(output, entries.size());
      for (Map.Entry<String, char[]> entry : fileDescriptors.entrySet()) {
         String key = entry.getKey();
         char[] value = entry.getValue();
         output.writeUTF(key);
         UnsignedNumeric.writeUnsignedInt(output, value.length);
         output.writeObject(compress(value));
      }
   }


   @Override
   public FileDescriptorSource readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      FileDescriptorSource fileDescriptorSource = new FileDescriptorSource();
      int size = UnsignedNumeric.readUnsignedInt(input);
      for (int i = 0; i < size; i++) {
         String name = input.readUTF();
         int length = UnsignedNumeric.readUnsignedInt(input);
         byte[] compressed = (byte[]) input.readObject();
         char[] contents = decompress(compressed, length);
         fileDescriptorSource.addProtoFile(name, String.valueOf(contents));
      }
      return fileDescriptorSource;
   }

   public byte[] compress(char[] input) throws IOException {
      ByteBuffer byteBuffer = UTF_8.encode(CharBuffer.wrap(input));
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           GZIPOutputStream zip = new GZIPOutputStream(baos);
           WritableByteChannel out = Channels.newChannel(zip)) {
         out.write(byteBuffer);
         zip.finish();
         return baos.toByteArray();
      }
   }

   private char[] decompress(byte[] input, int size) throws IOException {
      ByteBuffer result = ByteBuffer.allocate(size);
      try (ByteArrayInputStream in = new ByteArrayInputStream(input);
           GZIPInputStream gzip = new GZIPInputStream(in);
           ReadableByteChannel inputChannel = Channels.newChannel(gzip)) {
         inputChannel.read(result);
         result.flip();
         CharBuffer decoded = UTF_8.decode(result);
         return decoded.array();
      }
   }
}
