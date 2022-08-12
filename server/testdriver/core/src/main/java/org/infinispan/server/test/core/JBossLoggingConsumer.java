package org.infinispan.server.test.core;

import org.jboss.logging.BasicLogger;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class JBossLoggingConsumer extends BaseConsumer<JBossLoggingConsumer> {
   private final BasicLogger logger;
   private String prefix = "";

   public JBossLoggingConsumer(BasicLogger logger) {
      this.logger = logger;
   }

   public JBossLoggingConsumer withPrefix(String prefix) {
      this.prefix = "[" + prefix + "] ";
      return this;
   }

   @Override
   public void accept(OutputFrame outputFrame) {
      OutputFrame.OutputType outputType = outputFrame.getType();
      String utf8String = outputFrame.getUtf8String();
      utf8String = utf8String.replaceAll("((\\r?\\n)|(\\r))$", "");
      switch (outputType) {
         case STDOUT:
            this.logger.infof("%s%s: %s", prefix, outputType, utf8String);
            break;
         case STDERR:
            this.logger.errorf("%s%s: %s", prefix, outputType, utf8String);
            break;
         case END:
            break;
         default:
            throw new IllegalArgumentException("Unexpected outputType " + outputType);
      }
   }
}
