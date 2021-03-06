package org.smartsoftware.smartmap.domain.communication.request;

import org.smartsoftware.smartmap.domain.data.IKey;
import org.smartsoftware.smartmap.domain.data.IValue;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class PutRequest implements IRequest {

    private final IKey key;
    private final IValue value;

    public PutRequest(IKey key, IValue value) {
        this.key = key;
        this.value = value;
    }

    public IKey getKey() {
        return key;
    }

    public IValue getValue() {
        return value;
    }
}
