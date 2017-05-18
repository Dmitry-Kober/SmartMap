package org.smartsoftware.smartmap.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Created by dkober on 10.5.2017 г..
 */
public class PathFileContentCollector implements Collector<Path, Map<String, byte[]>, File> {

    private static final String NEWLINE = System.getProperty("line.separator");
    private static final Logger LOG = LoggerFactory.getLogger(PathFileContentCollector.class);
    private static final int THREAD_REGISTER_FLUSH_LIMIT = 10;

    private final Path targetFile;
    private final Path workingFolderPath;

    public PathFileContentCollector(Path workingFolderPath) {
        this.workingFolderPath = workingFolderPath;
        try {
            Path registerFilePath = Paths.get(workingFolderPath.toFile().getAbsolutePath() + "/register_" + System.currentTimeMillis());
            targetFile = Files.createFile(registerFilePath);
        }
        catch (IOException e) {
            LOG.error("Cannot create a register file for the '{}'.", workingFolderPath.toFile().getAbsolutePath(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Supplier<Map<String, byte[]>> supplier() {
        return HashMap<String, byte[]>::new;
    }

    @Override
    public BiConsumer<Map<String, byte[]>, Path> accumulator() {
        return (map, filePath) -> {
            String fileName = filePath.toFile().getName();
            try {
                byte[] value = Files.readAllBytes(filePath);
                map.put(fileName.substring(0, fileName.lastIndexOf(".data")), value);
            }
            catch (IOException e) {
                LOG.error("Cannot read the '{}' value file.", filePath.toFile().getAbsolutePath(), e);
            }

            if (map.size() == THREAD_REGISTER_FLUSH_LIMIT) {
                try {
                    StringBuilder mapSnapshot = map.entrySet().stream()
                            .collect(
                                    StringBuilder::new,
                                    (strBuilder, entry) -> strBuilder.append(entry.getKey()).append("$").append(new String(entry.getValue())).append(NEWLINE),
                                    (strBuilder1, strBuilder2) -> strBuilder1.append(strBuilder2.toString())
                            );
                    Files.write(targetFile, mapSnapshot.toString().getBytes(), StandardOpenOption.APPEND);
                    map.clear();
                }
                catch (IOException e) {
                    LOG.error("Cannot perform an intermediate flush for the '{}' register.", targetFile.toFile().getAbsolutePath(), e);
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public BinaryOperator<Map<String, byte[]>> combiner() {
        return (map1, map2) -> {
            map1.putAll(map2);
            return map1;
        };
    }

    @Override
    public Function<Map<String, byte[]>, File> finisher() {
        return (map) -> {
            StringBuilder mapSnapshot = map.entrySet().stream()
                    .collect(
                            StringBuilder::new,
                            (strBuilder, entry) -> strBuilder.append(entry.getKey()).append("$").append(new String(entry.getValue())).append(NEWLINE),
                            (strBuilder1, strBuilder2) -> strBuilder1.append(strBuilder2.toString())
                    );
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(targetFile.toFile().getAbsoluteFile()))) {
                bw.write(mapSnapshot.toString());
                map.clear();
            }
            catch (IOException e) {
                LOG.error("Cannot perform an intermediate flush for the '{}' register.", targetFile.toFile().getAbsolutePath(), e);
                throw new RuntimeException(e);
            }

            return targetFile.toFile();
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }

}
