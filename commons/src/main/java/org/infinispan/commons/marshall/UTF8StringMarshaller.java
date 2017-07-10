package org.infinispan.commons.marshall;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class UTF8StringMarshaller extends StringMarshaller {

   public UTF8StringMarshaller() {
      super(UTF_8);
   }

}
