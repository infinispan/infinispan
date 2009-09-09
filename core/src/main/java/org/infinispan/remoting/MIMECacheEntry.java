package org.infinispan.remoting;

import java.io.Serializable;
import java.util.Date;

/**
 * Wrapper object for entries that arrive via RESTful PUT/POST interface.
 * @author Michael Neale
 * @since 4.0
 */
public class MIMECacheEntry implements Serializable {

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


    /**
     * The date the entry was created...
     */
    public Date lastModified = new Date();

    public MIMECacheEntry() {}

    public MIMECacheEntry(String contentType, byte[] data) {
        this.contentType = contentType;
        this.data = data;
    }

}
