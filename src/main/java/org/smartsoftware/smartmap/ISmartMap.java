package org.smartsoftware.smartmap;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface ISmartMap {

    byte[] get(String key);
    boolean put(String key, byte[] value);
    boolean remove(String key);
    byte[] list();

}
