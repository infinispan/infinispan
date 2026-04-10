package org.infinispan.commands;

import org.infinispan.encoding.DataConverter;

/**
 * A command that can transform its data using a DataConverter.
 *
 * @since 16.2
 */
public interface DataConvertibleCommand {

   /**
    * Transforms the command's value using the provided data converter.
    *
    * @param dataConverter the data converter to use for value transformation
    */
   void transformValue(DataConverter dataConverter);

   /**
    * Transforms the command's result using the provided data converter.
    * This is used to convert the result from the local storage format to the
    * format expected by the requesting node.
    *
    * @param result the result to transform
    * @param dataConverter the data converter to use for result transformation
    * @return the transformed result
    */
   Object transformResult(Object result, DataConverter dataConverter);
}
