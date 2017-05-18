package org.smartsoftware.smartmap;

import org.junit.Test;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by dkober on 18.5.2017 г..
 */
public class SmartMapSmokeTest extends SmartMapTest {

    @Test
    public void shouldAddOneKeyValuePair() throws IOException {
        providedIPreparedSmartMap();
        smartMap.put("test_key_addition", "test_value_addition".getBytes());

        Path filePath = invokeDataFilePathBuilderMethodFor("test_key_addition");
        assertTrue(Files.exists(filePath));
        assertThat(new String(Files.readAllBytes(filePath)), equalTo("test_value_addition"));
    }

    @Test
    public void shouldGetExistingValue() {
        providedIPreparedSmartMap();
        smartMap.put("test_key_get", "test_value_get".getBytes());
        assertThat(new String(smartMap.get("test_key_get")), equalTo("test_value_get"));
    }

    @Test
    public void shouldRemoveExistingValue() {
        providedIPreparedSmartMap();
        smartMap.put("test_key_remove", "test_value_remove".getBytes());
        smartMap.put("test_key_remove_other", "test_value_remove_other".getBytes());

        smartMap.remove("test_key_remove");

        Path removeKeyFilePath = invokeDataFilePathBuilderMethodFor("test_key_remove");
        assertFalse(Files.exists(removeKeyFilePath));

        Path otherFilePath = invokeDataFilePathBuilderMethodFor("test_key_remove_other");
        assertTrue(Files.exists(otherFilePath));
    }

    @Test
    public void shouldListAllKeyValuePairs() throws IOException {
        providedIPreparedSmartMap();
        smartMap.put("list_key_1", "list_value_1".getBytes());
        smartMap.put("list_key_2", "list_value_2".getBytes());

        String register = new String(smartMap.list());
        assertTrue(register.contains("list_key_2$list_value_2"));
        assertTrue(register.contains("list_key_1$list_value_1"));
    }

    private Path invokeDataFilePathBuilderMethodFor(String key) {
        try {
            Method m = FileSystemManager.class.getDeclaredMethod("buildDataFilePath", String.class);
            m.setAccessible(true);
            return Paths.get((String) m.invoke(getInternalFileSystemManager(), key));
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private IFileSystemManager getInternalFileSystemManager() {
        try {
            Field fileSystemManagerField = smartMap.getClass().getDeclaredField("fileSystemManager");
            fileSystemManagerField.setAccessible(true);
            return (IFileSystemManager) fileSystemManagerField.get(smartMap);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
