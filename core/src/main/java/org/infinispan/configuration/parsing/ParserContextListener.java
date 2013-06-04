package org.infinispan.configuration.parsing;

import java.util.EventListener;

/**
 * ParserContextListener. An interface which should be implemented by listeners who wish to be
 * notified when a file has been successfully parsed. See {@link ParserContext}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ParserContextListener extends EventListener {
   void parsingComplete(ParserContext context);
}
