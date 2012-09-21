package org.infinispan.configuration.parsing;

/**
 * This class is here just for backward compatibility
 * @author <a href="mailto:tomaz.cerar@redhat.com">Tomaz Cerar</a>
 * @deprecated use {@link ParserRegistry}
 */
@Deprecated
public class Parser extends ParserRegistry {

    public Parser(ClassLoader classLoader) {
        super(classLoader);
    }
}
