package de.hpi.isg.sodap.flink.util;

import de.hpi.isg.sodap.flink.compression.GZIPInputStreamFactory;
import de.hpi.isg.sodap.flink.compression.InflaterInputStreamFactory;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * This class gathers a set of static methods that support activities related to file management.
 *
 * @author Sebastian Kruse
 */
public class FileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    private static final int MAX_CHAR_DETECTION_BUFFER_SIZE = 4096 * 4;

    private static final Map<String, InflaterInputStreamFactory<?>> COMPRESSION_EXTENSIONS = new HashMap<>();

    static {
        COMPRESSION_EXTENSIONS.put("gz", GZIPInputStreamFactory.getInstance());
        COMPRESSION_EXTENSIONS.put("gzip", GZIPInputStreamFactory.getInstance());
    }

    /**
     * Private constructor to avoid instantiation of this class.
     */
    private FileUtils() {
    }

    /**
     * Given a list of files and/or directories, it gathers all given files and those that are contained in the given
     * directories.
     *
     * @param depth      limits the traversal depth into subdirectories (-1: no depth restriction, 0: only gather given files, 1:
     *                   do not go into subdirectories, ...)
     * @param inputPaths is a list of file and directory paths
     * @return all gathered files as {@link Path} objects
     * @throws IOException
     * @throws {@link      FileNotFoundException} if a given input path is invalid
     */
    public static List<Path> gatherFiles(final int depth,
                                         final String... inputPaths) throws IOException {

        return gatherFiles(depth, createPaths(inputPaths));
    }

    /**
     * Given a list of files and/or directories, it gathers all given files and those that are contained in the given
     * directories.
     *
     * @param depth      limits the traversal depth into subdirectories (-1: no depth restriction, 0: only gather given files, 1:
     *                   do not go into subdirectories, ...)
     * @param inputPaths is a list of file and directory paths
     * @return all gathered files as {@link Path} objects
     * @throws IOException
     * @throws {@link      FileNotFoundException} if a given input path is invalid
     */

    public static List<Path> gatherFiles(final int depth,
                                         final Path... inputPaths) throws IOException {

        // Collector for gathered file paths.
        final Set<Path> gatheredFiles = new HashSet<Path>();

        // Follow each given path.
        for (final Path path : inputPaths) {

            // Sanity check: Given files must exist.
            final FileSystem fileSystem = path.getFileSystem();
            if (!fileSystem.exists(path)) {
                throw new FileNotFoundException("No such dir/file: " + path);
            }

            // Have the files gathered.
            gatherFilesAux(path, null, fileSystem, depth, gatheredFiles);
        }

        return new ArrayList<Path>(gatheredFiles);
    }

    /**
     * Recursive method for gathering files from a file system.
     *
     * @param path          is the base path to gather the files from
     * @param fileStatus    is the {@link FileStatus} of the path (can be <tt>null</tt>)
     * @param fs            is the {@link FileSystem} of the path
     * @param depth         is the remaining recursion depth
     * @param pathCollector collects the files
     * @throws IOException
     */
    private static void gatherFilesAux(final Path path, FileStatus fileStatus,
                                       final FileSystem fs, final int depth, final Set<Path> pathCollector)
            throws IOException {

        // Load file status if not given.
        if (fileStatus == null) {
            fileStatus = fs.getFileStatus(path);
        }

        if (fileStatus.isDir()) {

            // Check if we can further traverse.
            if (depth != 0) {

                // Traverse into directory.
                final FileStatus[] childStatuses = fs.listStatus(path);
                for (final FileStatus childStatus : childStatuses) {
                    gatherFilesAux(childStatus.getPath(), childStatus, fs,
                            Math.max(-1, depth - 1), pathCollector);
                }
            }

        } else {
            // Given path represents a file, so collect it.
            pathCollector.add(path);

        }
    }

    /**
     * Ensures that the given path points to an empty directory.
     *
     * @param parentPath is the location in which the empty directory is to be enforced
     * @param name is the name of the empty directory to be enforced
     * @param fs   is the {@link FileSystem} for the path or <tt>null</tt>
     * @return the path of the empty directory
     * @throws IOException
     */
    public static Path ensureEmptyDirectory(final Path parentPath,
                                            final String name, final FileSystem fs) throws IOException {
        final Path targetPath = new Path(parentPath, name);
        ensureEmptyDirectory(targetPath, fs);
        return targetPath;
    }

    /**
     * Ensures that the given path points to an empty directory.
     *
     * @param path is the location where the empty directory is to be enforced
     * @param fs   is the {@link FileSystem} for the path or <tt>null</tt>
     * @throws IOException
     */
    public static void ensureEmptyDirectory(final Path path, FileSystem fs)
            throws IOException {

        fs = ensureFileSystem(path, fs);

        // Delete file/dir+children if existent.
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        // Recreate the path.
        fs.mkdirs(path);
    }

    /**
     * Retrieves the {@link FileSystem} for the path if it is not already given.
     *
     * @param path for which the {@link FileSystem} is needed
     * @param fs   can be already given or <tt>null</tt>
     * @return the given {@link FileSystem} or the newly retrieved one
     * @throws IOException
     */
    public static FileSystem ensureFileSystem(final Path path,
                                              final FileSystem fs) throws IOException {
        if (fs == null) {
            return path.getFileSystem();
        }

        return fs;
    }

    /**
     * Reads the (logical) size of the specified file (in bytes). If the file is a directory, the size of all of its
     * contained files is added up.
     *
     * @param paths is the location to of the file
     * @return the length of the file
     * @throws IOException if reading or accessing the file fails
     */
    public static long getFileSize(FileSystem fs, final Path... paths)
            throws IOException {

        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException(String.format("Illegal paths: %s", Arrays.toString(paths)));
        }

        if (fs == null) {
            fs = paths[0].getFileSystem();
        }

        long size = 0;
        // This code leaves space for improvement:
        // Currently each file status is queried twice, it would
        // be nicer to avoid this by properly employing the visitor
        // pattern.
        final List<Path> files = gatherFiles(-1, paths);
        for (final Path file : files) {
            final FileStatus fileStatus = fs.getFileStatus(file);
            size += fileStatus.getLen();
        }

        return size;
    }

    /**
     * Creates a {@link Path} for each given input {@link String}.
     *
     * @param pathStrings is an array of path strings
     * @return an array of according {@link Path} objects
     */
    public static Path[] createPaths(final String... pathStrings) {
        final Path[] paths = new Path[pathStrings.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(pathStrings[i]);
        }
        return paths;
    }

    /**
     * Checks whether to paths are equal (neglecting issues like trailing slashes).
     * <p/>
     * <i>Careful, there still seem to be problems.</i>
     *
     * @param path1 is the first path to compare
     * @param path2 is the secont path to compare
     * @return whether the to paths are equal
     */
    public static boolean equals(final Path path1, final Path path2) {
        final URI uri1 = path1.toUri().normalize();
        final URI uri2 = path2.toUri().normalize();
        return uri1.equals(uri2);
    }

    /**
     * Normalizes the given path.
     *
     * @param path is the path to be normalized
     * @return the normalized path
     */
    public static Path normalize(final Path path) {
        return new Path(path.toUri().normalize());
    }

    /**
     * Removes the given file or directory.
     *
     * @param file      is the file or directory to remove
     * @param recursive tells whether the recursive removal shall be applied to directories
     * @return whether the deletion was successful
     * @throws IOException
     */
    public static boolean remove(final Path file, final boolean recursive) throws IOException {
        final FileSystem fs = file.getFileSystem();
        return fs.delete(file, recursive);
    }

    /**
     * Finds the most specific path that is a parent of all the given paths.
     *
     * @param filePaths are the paths for which a common parent is sought
     * @return the parent
     */
    public static String findCommonParentPath2(Collection<String> filePaths) {
        Path commonParentPath = null;
        for (String filePath : filePaths) {
            Path path = new Path(filePath);
            Path parent = path.getParent();
            if (commonParentPath == null) {
                commonParentPath = parent;
            } else {
                commonParentPath = findCommonParent(parent, commonParentPath);
            }
        }
        return commonParentPath.toString();
    }

    /**
     * Finds the most specific path that is a parent of all the given paths.
     *
     * @param files are the paths for which a common parent is sought
     * @return the parent
     */
    public static Path findCommonParentPath(Collection<Path> files) {
        Path commonParentPath = null;
        for (Path file : files) {
            Path parent = file.getParent();
            if (commonParentPath == null) {
                commonParentPath = parent;
            } else {
                commonParentPath = findCommonParent(parent, commonParentPath);
            }
        }
        return commonParentPath;
    }

    /**
     * Finds the most specific path that is a parrent of both given paths
     *
     * @param path1 is the first path
     * @param path2 is the second path
     * @return the common parent
     */
    public static Path findCommonParent(Path path1, Path path2) {
        // Bring paths to same depth.
        while (path1.depth() > path2.depth()) {
            path1 = path1.getParent();
        }
        while (path2.depth() > path1.depth()) {
            path2 = path2.getParent();
        }

        // Now ascend in both paths until we are at an equal node.
        while (!path1.equals(path2)) {
            path1 = path1.getParent();
            path2 = path2.getParent();
        }

        return path1;
    }

    /**
     * Finds the appropriate InflaterInputStreamFactory for the given file using the file extension.
     * @param file for which the InflaterInputStreamFactory is requested
     * @return the InflaterInputStreamFactory or {@code null} if none is found
     */
    public static InflaterInputStreamFactory<?> findInflaterInputStreamFactory(Path file) {
        String extension = extractExtension(file);
        InflaterInputStreamFactory<?> factory;
        return extension != null ? COMPRESSION_EXTENSIONS.get(extension) : null;
    }

    /**
     * Opens the given file for read access.
     *
     * @param file       that shall be opened
     * @param fileSystem is the filesystem of the file (optional)
     * @return an {@link FSDataInputStream} on the file
     * @throws IOException
     */
    public static InputStream open(final Path file, FileSystem fileSystem)
            throws IOException {

        fileSystem = FileUtils.ensureFileSystem(file, fileSystem);
        InputStream inputStream = fileSystem.open(file);
        InflaterInputStreamFactory<?> factory = findInflaterInputStreamFactory(file);
        if (factory != null) {
            inputStream = factory.create(inputStream);
        }
        return inputStream;
    }

    /**
     * Extracts the extension of a file path.
     *
     * @param file is a path that should contain an extension
     * @return the extension of the path or {@code null} if no extension was found
     */
    public static String extractExtension(final Path file) {
        String name = file.getName();
        int stopPosition = name.lastIndexOf(".");
        return stopPosition >= 0 ? name.substring(stopPosition + 1) : null;
    }

}
