package org.smartsoftware.smartmap.utils;

import java.util.function.Function;

/**
 * Created by dkober on 10.5.2017 г..
 */
@FunctionalInterface
public interface RuntimeExceptionThrowingWrapper<T,R> extends Function<T,R> {

    default R apply(T t) {
        try {
            return doApply(t);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    R doApply(T t) throws Exception;

    static<T,R> Function<T,R> wrap(RuntimeExceptionThrowingWrapper<T,R> f) {
        return f;
    }
}
