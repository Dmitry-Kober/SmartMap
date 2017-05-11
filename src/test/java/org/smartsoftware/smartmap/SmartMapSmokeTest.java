package org.smartsoftware.smartmap;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by dkober on 10.5.2017 г..
 */
public class SmartMapSmokeTest {

    private ISmartMap smartMap = new SmartMap();

    @Test
    public void shouldAddOneKeyValuePair() throws IOException {
        smartMap.put("test_key_addition", "test_value_addition".getBytes());

        Path filePath = Paths.get(SmartMap.WORKING_FOLDER + "/test_key_addition.data");
        assertTrue(Files.exists(filePath));
        assertThat(new String(Files.readAllBytes(filePath)), equalTo("test_value_addition"));
    }

    @Test
    public void shouldGetExistingValue() {
        smartMap.put("test_key_get", "test_value_get".getBytes());
        assertThat(new String(smartMap.get("test_key_get")), equalTo("test_value_get"));
    }

    @Test
    public void shouldRemoveExistingValue() {
        smartMap.put("test_key_remove", "test_value_remove".getBytes());
        smartMap.put("test_key_remove_other", "test_value_remove_other".getBytes());

        smartMap.remove("test_key_remove");

        Path removeKeyFilePath = Paths.get(SmartMap.WORKING_FOLDER + "/test_key_remove.data");
        assertFalse(Files.exists(removeKeyFilePath));

        Path otherFilePath = Paths.get(SmartMap.WORKING_FOLDER + "/test_key_remove_other.data");
        assertTrue(Files.exists(otherFilePath));
    }

    @Test
    public void shouldListAllKeyValuePairs() throws IOException {
        smartMap.put("list_key_1", "list_value_1".getBytes());
        smartMap.put("list_key_2", "list_value_2".getBytes());

        String register = new String(smartMap.list());
        assertTrue(register.contains("list_key_2$list_value_2"));
        assertTrue(register.contains("list_key_1$list_value_1"));
    }

    @After
    public void tearDown() throws IOException {
        Path workingFolderPath = Paths.get(SmartMap.WORKING_FOLDER);
        Files.list(workingFolderPath).forEach(file -> file.toFile().delete());
    }

}
