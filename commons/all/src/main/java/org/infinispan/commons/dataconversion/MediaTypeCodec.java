package org.infinispan.commons.dataconversion;

import java.io.UnsupportedEncodingException;

/**
 * MediaTypeCodecs handles extra content encoding and decoding specified as part of a {@link MediaType}.
 *
 * @since 13.0
 */
interface MediaTypeCodec {

   Object decodeContent(Object content, MediaType contentType) throws UnsupportedEncodingException;

   Object encodeContent(Object content, MediaType destinationType) throws UnsupportedEncodingException;
}
