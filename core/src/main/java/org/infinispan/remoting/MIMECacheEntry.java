package org.infinispan.remoting;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

/**
 * Wrapper object for entries that arrive via RESTful PUT/POST interface.
 * @author Michael Neale
 * @since 4.0
 */
public class MIMECacheEntry implements Serializable {

   private static final long serialVersionUID = -7857224258673285445L;

   /**
     * The MIME <a href="http://en.wikipedia.org/wiki/MIME">Content type</a>
     * value, for example application/octet-stream.
     * Often used in HTTP headers.
     */
    public String contentType;


    /**
     * The payload. The actual form of the contents depends on the contentType field.
     * Will be String data if the contentType is application/json, application/xml or text/*
     */
    public byte[] data;

    public MIMECacheEntry() {}

    public MIMECacheEntry(String contentType, byte[] data) {
        this.contentType = contentType;
        this.data = data;
    }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MIMECacheEntry)) return false;

      MIMECacheEntry that = (MIMECacheEntry) o;

      return !(contentType != null ? !contentType.equals(that.contentType) : that.contentType != null) && Arrays.equals(data, that.data);
   }

   @Override
   public int hashCode() {
      return 31 * (contentType != null ? contentType.hashCode() : 0) + (data != null ? Arrays.hashCode(data) : 0);
   }

   public static class Externalizer extends AbstractExternalizer<MIMECacheEntry> {

      @Override
      public Set<Class<? extends MIMECacheEntry>> getTypeClasses() {
         return Util.<Class<? extends MIMECacheEntry>>asSet(MIMECacheEntry.class);
      }

      @Override
      public void writeObject(ObjectOutput out, MIMECacheEntry obj) throws IOException {
         out.writeUTF(obj.contentType);
         out.writeInt(obj.data.length);
         out.write(obj.data);
      }

      @Override
      public MIMECacheEntry readObject(ObjectInput in) throws IOException, ClassNotFoundException {
         String contentType = in.readUTF();
         int len = in.readInt();
         byte[] data = new byte[len];
         in.readFully(data);
         return new MIMECacheEntry(contentType, data);
      }

      @Override
      public Integer getId() {
         return Ids.MIME_CACHE_ENTRY;
      }

   }

}
