package org.infinispan.marshall.protostream.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.WriteSkewException;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoName("Throwable")
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_THROWABLE)
public class MarshallableThrowable {

    private volatile Throwable throwable;

    private static final Set<Class<?>> MARSHALLABLE_EXCEPTIONS = new HashSet<>();
    static {
        MARSHALLABLE_EXCEPTIONS.add(WriteSkewException.class);
    }

    @ProtoField(1)
    final WrappedMessage wrappedThrowable;

    @ProtoField(number = 2, name = "implementation")
    final String impl;

    @ProtoField(number = 3, name = "message")
    final String msg;

    @ProtoField(4)
    final MarshallableThrowable cause;

    @ProtoField(5)
    final ArrayList<MarshallableThrowable> suppressed;

    @ProtoFactory
    MarshallableThrowable(WrappedMessage wrappedThrowable, String impl, String msg, MarshallableThrowable cause, ArrayList<MarshallableThrowable> suppressed) {
        this.throwable = WrappedMessages.unwrap(wrappedThrowable);
        this.wrappedThrowable = wrappedThrowable;
        this.impl = impl;
        this.msg = msg;
        this.cause = cause;
        this.suppressed = suppressed;
    }

    public static MarshallableThrowable create(Throwable t) {
        if (t == null)
            return null;

        if (MARSHALLABLE_EXCEPTIONS.contains(t.getClass()))
            return new MarshallableThrowable(new WrappedMessage(t), null, null, null, null);

        var suppressed = t.getSuppressed().length == 0 ? null : Stream.of(t.getSuppressed()).map(MarshallableThrowable::create).collect(Collectors.toCollection(ArrayList::new));
        return new MarshallableThrowable(null, t.getClass().getName(), t.getMessage(), create(t.getCause()), suppressed);
    }

    public static Throwable unwrap(MarshallableThrowable t) {
        return t == null ? null : t.get();
    }

    public Throwable get() {
        if (throwable == null) {
            throwable = recreateGenericThrowable(impl, msg, cause);
            if (suppressed != null) suppressed.forEach(s -> throwable.addSuppressed(s.get()));
        }
        return throwable;
    }

    private Throwable recreateGenericThrowable(String impl, String msg, MarshallableThrowable t) {
        Throwable cause = t == null ? null : t.get();
        try {
            Class<?> clazz = Class.forName(impl);

            Object retVal;
            if (cause == null && msg == null) {
                retVal = create(clazz, this::getInstance);
            } else if (cause == null) {
                retVal = create(clazz, c -> getInstance(c, msg), String.class);
            } else if (msg == null) {
                retVal = create(clazz, c -> getInstance(c, cause), Throwable.class);
            } else {
                retVal = create(clazz, c -> getInstance(c, msg, cause), String.class, Throwable.class);
                if (retVal == null) {
                    retVal = create(clazz, c -> getInstance(c, cause), Throwable.class);
                }
            }
            return (Throwable) retVal;
        } catch (ClassNotFoundException e) {
            throw new MarshallingException(e);
        }
    }

    private Object create(Class<?> clazz, Function<Constructor<?>, ?> builder, Class<?>... args) {
        try {
            Constructor<?> ctor = clazz.getConstructor(args);
            return builder.apply(ctor);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private Object getInstance(Constructor<?> constructor, Object... args) {
        try {
            return constructor.newInstance(args);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new MarshallingException(e);
        }
    }

    @Override
    public String toString() {
        return get().toString();
    }
}
