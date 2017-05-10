package org.smartsoftware.smartmap;

import java.io.File;
import java.util.Collection;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface ISmartMap {

    byte[] get(String key);
    void put(String key, byte[] value);
    void remove(String key);
    byte[] list();

}
