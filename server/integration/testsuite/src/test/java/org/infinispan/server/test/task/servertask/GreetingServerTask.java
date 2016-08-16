package org.infinispan.server.test.task.servertask;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Optional;

import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * ServerTask getting a custom mojo as parameter and processing it.
 *
 * @author amanukya
 */
public class GreetingServerTask implements ServerTask {

    public static final String NAME = "testTask";
    private TaskContext taskContext;

    @Override
    @SuppressWarnings("unchecked")
    public Object call() throws IOException, ClassNotFoundException {
        Greeting params = fromBytes((byte[]) taskContext.getParameters().get().get("greeting"));

        return params.getGreeting();
    }

    @Override
    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<String> getAllowedRole() {
        return Optional.empty();
    }

    private <T> T fromBytes(byte[] bytes) {
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(is);
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
