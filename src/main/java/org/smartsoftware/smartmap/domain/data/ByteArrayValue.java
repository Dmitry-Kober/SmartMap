package org.smartsoftware.smartmap.domain.data;

import java.util.Optional;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class ByteArrayValue implements IValue {

    private final byte[] data;

    public ByteArrayValue() {
        data = null;
    }

    public ByteArrayValue(byte[] data) {
        this.data = data;
    }

    @Override
    public Optional<byte[]> get() {
        return Optional.ofNullable(data);
    }

}
