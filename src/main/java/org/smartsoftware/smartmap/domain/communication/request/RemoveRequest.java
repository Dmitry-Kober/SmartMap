package org.smartsoftware.smartmap.domain.communication.request;

import org.smartsoftware.smartmap.domain.data.IKey;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class RemoveRequest implements IRequest {

    private final IKey key;

    public RemoveRequest(IKey key) {
        this.key = key;
    }

    public IKey getKey() {
        return key;
    }
}
