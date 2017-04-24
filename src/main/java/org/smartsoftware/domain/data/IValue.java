package org.smartsoftware.domain.data;

import java.util.Optional;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IValue {

    default Optional<byte[]> get() {
        return Optional.empty();
    }

}
