package org.infinispan.configuration.parsing;

/**
 * ParserContext. By using the methods declared in this interface, parsers can register listeners
 * which will be invoked when the parsing completes successfully. This is useful when configuration can
 * be completed only when the whole file has been parsed (e.g. because of the use of named references)
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ParserContext {
   void addParsingCompleteListener(ParserContextListener l);

   void fireParsingComplete();
}
