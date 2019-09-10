package org.infinispan.client.rest.impl.okhttp;

import okhttp3.RequestBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestEntityAdaptorOkHttp {
   RequestBody toRequestBody();
}
