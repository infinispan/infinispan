package org.infinispan.tasks.logging;

import static org.jboss.logging.Messages.getBundle;

import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Messages for the tasks module
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MessageBundle(projectCode = "ISPN")
public interface Messages {
    Messages MESSAGES = getBundle(Messages.class);

    @Message(value = "Task %s completed successfully", id = 101000)
    String taskSuccess(String name);

    @Message(value = "Task %s completed with errors", id = 101001)
    String taskFailure(String name);
}
