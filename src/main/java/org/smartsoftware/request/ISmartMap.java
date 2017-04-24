package org.smartsoftware.request;

import org.smartsoftware.domain.data.Entry;
import org.smartsoftware.domain.data.IKey;
import org.smartsoftware.domain.data.IValue;

import java.util.Collection;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface ISmartMap {

    IValue get(IKey key);
    Collection<Entry> list(); // TODO: consider memory overflowing here!!!

    void put(IKey key, IValue value);
    void remove(IKey key);

}
