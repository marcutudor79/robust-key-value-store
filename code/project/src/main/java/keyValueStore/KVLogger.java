package keyValueStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class KVLogger {
    private static final Logger LOGGER = Logger.getLogger("KVLog");
    private static volatile boolean initialized = false;

    private KVLogger() {}

    private static synchronized void initIfNeeded() {
        if (initialized) return;
        String path = System.getProperty("kv.log");
        if (path == null || path.isEmpty()) {
            String env = System.getenv("KV_LOG");
            path = (env != null && !env.isEmpty()) ? env : "kv_store.log";
        }
        try {
            Path p = Paths.get(path).toAbsolutePath();
            Path parent = p.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            // overwrite file on each run
            FileHandler fh = new FileHandler(p.toString(), false);
            fh.setFormatter(new SimpleFormatter());
            LOGGER.setUseParentHandlers(false);
            LOGGER.setLevel(Level.INFO);
            LOGGER.addHandler(fh);
            initialized = true;
        } catch (IOException e) {
            // Fallback to console if file setup fails
            LOGGER.log(Level.WARNING, "KVLogger initialization failed: " + e.getMessage());
        }
    }

    public static void log(String message) {
        initIfNeeded();
        LOGGER.info(message);
    }
}
