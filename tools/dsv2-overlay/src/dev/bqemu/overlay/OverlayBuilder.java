package dev.bqemu.overlay;

// This builder patches one released connector class without cloning or
// rebuilding upstream. Every behavior-changing descriptor is locked beside the
// tool and is checked before bytecode is emitted.
//
// Upstream contracts:
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/DataSourceWriterContext.java#L38-L50
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/719817782a214b8ca72be520870013a3e0253d92/spark-bigquery-connector-common/src/main/java/com/google/cloud/spark/bigquery/write/context/BigQueryDirectDataSourceWriterContext.java#L199-L312
// https://spark.apache.org/docs/3.5.8/api/java/org/apache/spark/sql/connector/write/streaming/StreamingWrite.html
// Javassist API: https://www.javassist.org/html/javassist/CtNewMethod.html

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.CRC32;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.bytecode.CodeAttribute;

public final class OverlayBuilder {
    private static final String OPERATION = "build-dsv2-streaming-overlay";
    private static final String COMMIT_HOOK = "onDataStreamingWriterCommit";
    private static final String ABORT_HOOK = "onDataStreamingWriterAbort";

    private OverlayBuilder() {}

    public static void main(String[] args) {
        try {
            build(parseArgs(args));
        } catch (Drift error) {
            event(error.stage, error.shape, error.fingerprint, "failed", error.fixHint);
            System.exit(1);
        } catch (Throwable error) {
            event(
                    "builder",
                    error.getClass().getSimpleName(),
                    digest(error.getClass().getName().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    "failed",
                    "inspect-version-locked-builder-contract");
            System.exit(1);
        }
    }

    private static void build(Map<String, String> values) throws Exception {
        requireKeys(values);
        Path input = Path.of(values.get("input"));
        Path output = Path.of(values.get("output"));
        byte[] inputBytes = Files.readAllBytes(input);
        requireDigest("input-artifact", "maven-jar", inputBytes,
                parseLong(values, "input-size"), values.get("input-sha"));

        byte[] originalClass = readEntry(input, values.get("target-entry"));
        requireDigest("target-class", "class-entry", originalClass,
                parseLong(values, "target-size"), values.get("target-sha"));

        ClassPool pool = new ClassPool(false);
        pool.appendSystemPath();
        pool.insertClassPath(input.toString());
        CtClass target = pool.get(values.get("target-class"));
        if (target.isInterface() || target.isAnnotation() || target.isEnum()) {
            throw new Drift("target-class", "unsupported-kind", digest(originalClass),
                    "review-target-class-contract");
        }

        verifyRequiredMethod(target, values, "commit");
        verifyRequiredMethod(target, values, "abort");
        verifyAbsent(target, values.get("commit-hook"), values.get("commit-hook-desc"));
        verifyAbsent(target, values.get("abort-hook"), values.get("abort-hook-desc"));

        addDelegate(target, COMMIT_HOOK, "commit");
        addDelegate(target, ABORT_HOOK, "abort");
        verifyPatchedMethod(target, values, "commit-hook");
        verifyPatchedMethod(target, values, "abort-hook");

        byte[] patchedClass = target.toBytecode();
        target.detach();
        requireDigest("output-class", "patched-class", patchedClass,
                parseLong(values, "output-class-size"), values.get("output-class-sha"));
        writeOneClassJar(output, values.get("target-entry"), patchedClass);
        event("output", "one-class-overlay-jar", digest(Files.readAllBytes(output)),
                "built", "none");
    }

    private static void verifyRequiredMethod(
            CtClass target, Map<String, String> values, String key) throws Exception {
        String name = values.get(key + "-name");
        String descriptor = values.get(key + "-desc");
        CtMethod method = findDeclared(target, name, descriptor);
        if (method == null) {
            throw new Drift("method-descriptor", key + ":missing",
                    digest((name + "\0" + descriptor).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    "review-connector-version-and-method-descriptor");
        }
        CodeAttribute code = method.getMethodInfo2().getCodeAttribute();
        if (code == null) {
            throw new Drift("method-bytecode", key + ":abstract-or-native", "sha256:none",
                    "review-target-method-implementation");
        }
        requireDigest("method-bytecode", key, code.getCode(),
                parseLong(values, key + "-code-size"), values.get(key + "-code-sha"));
    }

    private static void verifyPatchedMethod(
            CtClass target, Map<String, String> values, String key) throws Exception {
        CtMethod method = findDeclared(target, values.get(key), values.get(key + "-desc"));
        if (method == null) {
            throw new Drift("patched-method", key + ":missing", "sha256:none",
                    "inspect-javassist-patch-contract");
        }
        CodeAttribute code = method.getMethodInfo2().getCodeAttribute();
        requireDigest("patched-bytecode", key, code.getCode(),
                parseLong(values, key + "-code-size"), values.get(key + "-code-sha"));
    }

    private static void verifyAbsent(CtClass target, String name, String descriptor) {
        if (findDeclared(target, name, descriptor) != null) {
            throw new Drift("precondition", name + ":already-declared",
                    digest((name + "\0" + descriptor).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    "remove-overlay-or-review-new-upstream-implementation");
        }
    }

    private static CtMethod findDeclared(CtClass target, String name, String descriptor) {
        return Arrays.stream(target.getDeclaredMethods())
                .filter(method -> method.getName().equals(name))
                .filter(method -> method.getMethodInfo2().getDescriptor().equals(descriptor))
                .findFirst()
                .orElse(null);
    }

    private static void addDelegate(CtClass target, String hook, String delegate) throws Exception {
        String context = "com.google.cloud.spark.bigquery.write.context.WriterCommitMessageContext";
        String guard = delegate.equals("commit")
                ? "if (this.writeAtLeastOnce || this.tableToWrite.toDeleteOnAbort()) {"
                    + " throw new IllegalStateException(\"DSv2 overlay requires exact append to a pre-existing table\");"
                    + " }"
                : "";
        String source = "public void " + hook + "(long epochId, " + context
                + "[] messages) { " + guard + " this." + delegate + "(messages); }";
        target.addMethod(CtNewMethod.make(source, target));
    }

    private static byte[] readEntry(Path jarPath, String entryName) throws IOException {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(entryName);
            if (entry == null || entry.isDirectory()) {
                throw new Drift("target-class", "entry-missing", "sha256:none",
                        "review-target-class-entry");
            }
            return jar.getInputStream(entry).readAllBytes();
        }
    }

    private static void writeOneClassJar(Path output, String entryName, byte[] classBytes)
            throws IOException {
        Files.createDirectories(output.toAbsolutePath().getParent());
        byte[] entryBytes = entryName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(classBytes);
        int localSize = 30 + entryBytes.length + classBytes.length;
        int centralSize = 46 + entryBytes.length;

        // ZipEntry time setters depend on the host timezone and may add an
        // extended timestamp field. Emit the tiny STORED archive directly so
        // CI and developer hosts produce identical bytes. The fixed DOS date
        // is 1980-01-01 and all optional fields are empty.
        // Format: https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
        try (OutputStream zip = Files.newOutputStream(output)) {
            writeU32(zip, 0x04034b50L);
            writeU16(zip, 10);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 33);
            writeU32(zip, crc.getValue());
            writeU32(zip, classBytes.length);
            writeU32(zip, classBytes.length);
            writeU16(zip, entryBytes.length);
            writeU16(zip, 0);
            zip.write(entryBytes);
            zip.write(classBytes);

            writeU32(zip, 0x02014b50L);
            writeU16(zip, 20);
            writeU16(zip, 10);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 33);
            writeU32(zip, crc.getValue());
            writeU32(zip, classBytes.length);
            writeU32(zip, classBytes.length);
            writeU16(zip, entryBytes.length);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU32(zip, 0);
            writeU32(zip, 0);
            zip.write(entryBytes);

            writeU32(zip, 0x06054b50L);
            writeU16(zip, 0);
            writeU16(zip, 0);
            writeU16(zip, 1);
            writeU16(zip, 1);
            writeU32(zip, centralSize);
            writeU32(zip, localSize);
            writeU16(zip, 0);
        }
    }

    private static void writeU16(OutputStream output, long value) throws IOException {
        output.write((int) value & 0xff);
        output.write((int) (value >>> 8) & 0xff);
    }

    private static void writeU32(OutputStream output, long value) throws IOException {
        writeU16(output, value);
        writeU16(output, value >>> 16);
    }

    private static void requireDigest(
            String stage, String shape, byte[] payload, long expectedSize, String expectedSha) {
        String actual = digest(payload);
        if (payload.length != expectedSize || !actual.equals("sha256:" + expectedSha)) {
            throw new Drift(stage, shape + ":bytes:" + payload.length, actual,
                    "refresh-only-after-source-and-bytecode-review");
        }
    }

    private static long parseLong(Map<String, String> values, String key) {
        try {
            return Long.parseLong(values.get(key));
        } catch (RuntimeException error) {
            throw new Drift("arguments", key + ":not-integer", "sha256:none",
                    "use-the-reviewed-overlay-lock");
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        if (args.length == 0 || args.length % 2 != 0) {
            throw new Drift("arguments", "key-value-pairs", "sha256:none",
                    "invoke-through-build-script");
        }
        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            String key = args[index];
            if (!key.startsWith("--") || values.put(key.substring(2), args[index + 1]) != null) {
                throw new Drift("arguments", "duplicate-or-invalid-key", "sha256:none",
                        "invoke-through-build-script");
            }
        }
        return values;
    }

    private static void requireKeys(Map<String, String> values) {
        Set<String> expected = new LinkedHashSet<>(Arrays.asList(
                "input", "output", "input-size", "input-sha", "target-class", "target-entry",
                "target-size", "target-sha", "commit-name", "commit-desc", "commit-code-size",
                "commit-code-sha", "abort-name", "abort-desc", "abort-code-size", "abort-code-sha",
                "commit-hook", "commit-hook-desc", "commit-hook-code-size", "commit-hook-code-sha",
                "abort-hook", "abort-hook-desc", "abort-hook-code-size", "abort-hook-code-sha",
                "output-class-size", "output-class-sha"));
        if (!values.keySet().equals(expected)) {
            throw new Drift("arguments", "key-set-drift", "sha256:none",
                    "invoke-through-build-script");
        }
    }

    private static String digest(byte[] payload) {
        try {
            byte[] hashed = MessageDigest.getInstance("SHA-256").digest(payload);
            StringBuilder encoded = new StringBuilder(hashed.length * 2);
            for (byte value : hashed) {
                encoded.append(String.format("%02x", value & 0xff));
            }
            return "sha256:" + encoded;
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    private static void event(
            String stage, String shape, String fingerprint, String status, String fixHint) {
        System.out.println("version=0.44.2 operation=" + OPERATION + " stage=" + stage
                + " shape=" + shape + " fingerprint=" + fingerprint + " status=" + status
                + " fix_hint=" + fixHint);
    }

    private static final class Drift extends RuntimeException {
        private final String stage;
        private final String shape;
        private final String fingerprint;
        private final String fixHint;

        private Drift(String stage, String shape, String fingerprint, String fixHint) {
            this.stage = stage;
            this.shape = shape;
            this.fingerprint = fingerprint;
            this.fixHint = fixHint;
        }
    }
}
