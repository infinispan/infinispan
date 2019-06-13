package org.infinispan.configuration.parsing;

import java.io.IOException;
import java.net.URL;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface XMLResourceResolver {
   URL resolveResource(String href) throws IOException;
}
