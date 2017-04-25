package org.smartsoftware.smartmap.domain.communication.response;

import org.smartsoftware.smartmap.domain.data.IValue;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class ValueResponse implements IResponse {

    private final IValue value;

    public ValueResponse(IValue value) {
        this.value = value;
    }

    public IValue getValue() {
        return value;
    }
}
