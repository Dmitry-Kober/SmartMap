package org.smartsoftware.smartmap.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Created by dkober on 10.5.2017 г..
 */
public class FileToFileCollector implements Collector<File, File, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(FileToFileCollector.class);
    private static final String NEWLINE = System.getProperty("line.separator");

    @Override
    public Supplier<File> supplier() {
        return () -> {
            try {
                return Files.createFile(Paths.get("global_register_" + System.currentTimeMillis())).toFile();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public BiConsumer<File, File> accumulator() {
        return  (accumulator, value) -> {
            try ( BufferedWriter bufferedWriter = Files.newBufferedWriter(accumulator.toPath(), StandardOpenOption.APPEND) ) {
                Files.lines(value.toPath())
                    .forEach(line -> {
                        try {
                            bufferedWriter.write(line + NEWLINE);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public BinaryOperator<File> combiner() {
        return  (file1, file2) -> {
            try ( BufferedWriter bufferedWriter = Files.newBufferedWriter(file1.toPath(), StandardOpenOption.APPEND) ) {
                Files.lines(file2.toPath()).forEach(line -> {
                    try {
                        bufferedWriter.write(line);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return file1;
        };
    }

    @Override
    public Function<File, byte[]> finisher() {
        return file -> {
            try {
                return Files.readAllBytes(file.toPath());
            }
            catch (IOException e) {
                return new byte[0];
            }
            finally {
                try {
                    Files.deleteIfExists(file.toPath());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Characteristics.UNORDERED);
    }
}
