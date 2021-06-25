package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

/**
 * Encodes/decodes Java primitives, byte arrays and String as text.
 *
 * <br/> It only applies to {@link MediaType#APPLICATION_OBJECT} content with a "type" parameter.
 *
 * The following values are supported for the "type" param:
 * <br/>
 * <ul>
 *    <li>java.lang.String</li>
 *    <li>java.lang.String</li>
 *    <li>java.lang.Boolean</li>
 *    <li>java.lang.Short</li>
 *    <li>java.lang.Byte</li>
 *    <li>java.lang.Integer</li>
 *    <li>java.lang.Long</li>
 *    <li>java.lang.Float</li>
 *    <li>java.lang.Double</li>
 *    <li>ByteArray</li>
 * </ul>
 *
 * When "ByteArray" is used, it implies the content is encoded as a <a href="https://www.ietf.org/rfc/rfc4648.txt">RFC 4648</a>
 * hexadecimal prefixed by "0x". For all other supported types, it will use {@link Object#toString()}.
 *
 * @since 13.0
 */
class JavaMediaTypeCodec implements MediaTypeCodec {

   @Override
   public Object decodeContent(Object content, MediaType contentType) {
      String type = contentType.getClassType();
      if (!contentType.match(APPLICATION_OBJECT) || type == null) {
         return content;
      }
      String strContent;
      if (content instanceof byte[]) {
         strContent = new String((byte[]) content, contentType.getCharset());
      } else if (content instanceof String) {
         strContent = content.toString();
      } else {
         return content;
      }
      if (content.getClass().getName().equals(type)) {
         return content;
      }
      JavaStringCodec codec = JavaStringCodec.forType(type);
      if (codec == null) {
         throw new EncodingException("Type " + type + " is unsupported");
      }
      return codec.decode(strContent);
   }

   @Override
   public Object encodeContent(Object content, MediaType destinationType) {
      String type = destinationType.getClassType();
      if (!destinationType.match(APPLICATION_OBJECT) || type == null) {
         return content;
      }
      if (content.getClass().getName().equals(type)) {
         return content;
      }
      JavaStringCodec codec = JavaStringCodec.forType(type);
      return codec.encode(content, destinationType);
   }
}
