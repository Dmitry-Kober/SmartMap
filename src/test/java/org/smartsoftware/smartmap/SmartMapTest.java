package org.smartsoftware.smartmap;

import org.junit.AfterClass;
import org.junit.Before;
import org.smartsoftware.smartmap.filesystem.FileSystemManager;
import org.smartsoftware.smartmap.filesystem.IFileSystemManager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by dkober on 18.5.2017 г..
 */
public class SmartMapTest {

    protected ISmartMap smartMap;

    @Before
    public void setUp() {
        smartMap = null;
    }

    protected void providedIPreparedSmartMap() {
        IFileSystemManager originalFileSystemManger = new FileSystemManager("repository_" + System.currentTimeMillis());
        smartMap = new SmartMap(originalFileSystemManger);
    }

    @AfterClass
    public static void tearDown() throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Files
                .find(Paths.get("."), 1, (path, basicFileAttributes) -> path.toFile().isDirectory() && path.toFile().getName().startsWith("repository"))
                .forEach(path -> {
                    removeFilesInFolder(path.toFile());
                });
    }

    private static void removeFilesInFolder(File folder) {
        for (File file : folder.listFiles()) {
            if (file.isDirectory()) {
                removeFilesInFolder(file);
            }
            file.delete();
        }
        folder.delete();
    }
}
