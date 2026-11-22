/*
 * Copyright 2026 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tripatha.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.tripatha.api.RdfCloudTripleStoreConstants;
import org.tripatha.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.tripatha.api.domain.RyaSchema;
import org.tripatha.api.domain.RyaStatement;
import org.tripatha.api.resolver.RyaTripleContext;
import org.tripatha.loader.BulkBuild;
import org.tripatha.loader.LoaderMain;
import org.tripatha.loader.LoaderOptions;
import org.tripatha.loader.StoreCheck;
import org.tripatha.loader.gen.EntityGenerator;
import org.tripatha.store.EvalStatsMutations;
import org.tripatha.store.RyaStoreDAO;
import org.tripatha.store.StoreRdfConfiguration;
import org.tripatha.store.spi.Auths;
import org.tripatha.store.spi.Cell;
import org.tripatha.store.spi.CellScanner;
import org.tripatha.store.spi.KeyRange;
import org.tripatha.store.spi.ScanSpec;
import org.tripatha.test.StoreITBase;
import org.tripatha.test.accumulo.MiniAccumuloSingleton;

/**
 * The Spark build on local Spark and the test cluster: for the generator it writes every cell
 * the loader's own build writes; for RDF files every cell a DAO load of the same statements
 * writes, blank nodes read across splits, graphs, triple terms, labels and bad lines included.
 */
class SparkBuildIT extends StoreITBase {
    private static final String[] AUTHS = {"A", "B"};
    private static JavaSparkContext sc;

    @TempDir
    Path dir;

    @BeforeAll
    static void startSpark() {
        sc = new JavaSparkContext(SparkBuild.configure(new SparkConf().setMaster("local[3]").setAppName("SparkBuildIT")
                .set("spark.ui.enabled", "false").set("spark.default.parallelism", "3")));
    }

    @AfterAll
    static void stopSpark() {
        if (sc != null) {
            sc.close();
        }
    }

    @BeforeEach
    void accumuloOnly() {
        assumeBackend("accumulo");
        grantAuthorizations(AUTHS);
    }

    private static Map<String, String> env() {
        final var cluster = MiniAccumuloSingleton.getCluster();
        return Map.of("TRIPATHA_ACCUMULO_ZOOKEEPERS", cluster.getZooKeepers(), "TRIPATHA_ACCUMULO_INSTANCE", cluster.getInstanceName(),
                "TRIPATHA_ACCUMULO_USER", MiniAccumuloSingleton.ROOT_USER, "TRIPATHA_ACCUMULO_PASSWORD",
                MiniAccumuloSingleton.ROOT_PASSWORD);
    }

    /** Runs the Spark build on the test's store; returns what it printed. */
    private String spark(final String... args) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        new SparkBuild(sc, getStore(), SparkBuild.Options.parse(args, env()), new PrintStream(out, true, StandardCharsets.UTF_8)).run();
        return out.toString(StandardCharsets.UTF_8);
    }

    private static String loader(final String command, final List<String> args) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final String[] all = Stream.concat(Stream.of(command), args.stream()).toArray(String[]::new);
        final int code = LoaderMain.run(all, env(), new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(out, true, StandardCharsets.UTF_8));
        final String text = out.toString(StandardCharsets.UTF_8);
        assertEquals(0, code, text);
        return text;
    }

    // ------------------------------------------------------------------ the generator

    private List<String> dataset(final String prefix, final String... extra) {
        final List<String> a = new ArrayList<>(List.of("--table-prefix", prefix, "--entities", "2000", "--seed", "7", "--labels", "A,B",
                "--labelled-fraction", "0.3", "--graphs", "4", "--rdf12-fraction", "0.3", "--text-index", "--geo-index",
                "--entity-index", "--entity-shards", "4", "--block-size", "25"));
        a.addAll(List.of(extra));
        return a;
    }

    @Test
    void generateWritesWhatTheLoadersBuildWrites() throws Exception {
        final String built = getTablePrefix() + "b_";
        final String sparked = getTablePrefix();
        for (final String prefix : List.of(built, sparked)) {
            loader("prepare", dataset(prefix, "--splits", "7"));
        }
        loader("build", dataset(built, "--threads", "4", "--work-dir", dir.resolve("build").toString()));

        final List<String> spark = new ArrayList<>(List.of("generate"));
        spark.addAll(dataset(sparked, "--work-dir", dir.resolve("spark").toString(), "--file-size", "0.05", "--tasks", "5"));
        final String out = spark(spark.toArray(new String[0]));
        assertTrue(out.contains("Import: "), out);

        for (final String table : List.of("spo", "po", "osp", "text", "geo", "entity")) {
            assertSame(table, cells(built + table, true), cells(sparked + table, true));
        }
        for (final String table : List.of("eval", "textcount", "graphs")) {
            assertSame(table, cells(built + table, false), cells(sparked + table, false));
        }
        // Several files per tablet, and the entity index's shard rows spread over files.
        for (final String table : List.of("spo", "entity")) {
            final String[] plan = out.lines().filter(l -> l.trim().startsWith(sparked + table + " ")).findFirst().orElseThrow().trim()
                    .split("\\s+");
            assertTrue(Integer.parseInt(plan[4]) > Integer.parseInt(plan[3]), table + ": " + String.join(" ", plan));
        }

        // Done: importing again imports nothing, and writing again is refused (it would count twice).
        spark.addAll(List.of("--phase", "import"));
        assertTrue(spark(spark.toArray(new String[0])).contains("Import: done already."));
        final List<String> write = new ArrayList<>(spark.subList(0, spark.size() - 2));
        write.addAll(List.of("--phase", "write"));
        assertThrows(StoreCheck.Mismatch.class, () -> spark(write.toArray(new String[0])));
    }

    /**
     * Every way of building the generated dataset reads, counter by counter and for every reader,
     * as a load that writes every counter ({@code --stats-ones keep}): a load that leaves subject
     * counters of 1 out, the loader's build, and Spark's generate in one part and in two. A 1 may
     * stay out only when it is its counter's only cell and nothing else counts that counter: not
     * the reference data {@code prepare} wrote, not another label's cell, not another part.
     */
    @Test
    void everyBuildReadsAsALoadOfEveryCounter() throws Exception {
        final String truth = getTablePrefix() + "k_";
        final String loaded = getTablePrefix() + "l_";
        final String built = getTablePrefix() + "b_";
        final String whole = getTablePrefix() + "w_";
        final String parts = getTablePrefix();
        loader("prepare", dataset(truth, "--splits", "7", "--stats-ones", "keep"));
        final Map<String, Long> referenceOnly = reads(truth + "eval", AUTHS);
        loader("load", dataset(truth, "--threads", "4", "--stats-ones", "keep", "--progress-file", dir.resolve("k.progress").toString()));
        for (final String prefix : List.of(loaded, built, whole, parts)) {
            loader("prepare", dataset(prefix, "--splits", "7"));
        }
        loader("load", dataset(loaded, "--threads", "4", "--progress-file", dir.resolve("l.progress").toString()));
        loader("build", dataset(built, "--threads", "4", "--work-dir", dir.resolve("build").toString()));
        final List<String> one = new ArrayList<>(List.of("generate"));
        one.addAll(dataset(whole, "--work-dir", dir.resolve("spark-w").toString(), "--file-size", "0.05", "--tasks", "5"));
        assertTrue(spark(one.toArray(new String[0])).contains("Import: "));
        // Two parts into one store, each with a work directory of its own.
        for (int part = 0; part < 2; part++) {
            final List<String> spark = new ArrayList<>(List.of("generate"));
            spark.addAll(dataset(parts, "--part", Integer.toString(part), "--parts", "2", "--work-dir",
                    dir.resolve("spark-p" + part).toString(), "--file-size", "0.05", "--tasks", "3"));
            assertTrue(spark(spark.toArray(new String[0])).contains("Import: "));
        }

        // The dataset holds both cases a 1 must stay for, or this test would pass trivially: a counter
        // the reference data counts that the entities add exactly 1 to (a class's label word in an
        // entity's label), and a counter whose 1 sits next to another label's cell.
        final Map<String, Long> truthReads = reads(truth + "eval", AUTHS);
        // The reference data's counters, found here on their own (not through the code under test).
        final Set<String> referenceCounters = new HashSet<>();
        for (final RyaStatement st : org.tripatha.loader.gen.ReferenceData.statements(new EntityGenerator(
                LoaderOptions.parse(Stream.concat(Stream.of("load"), dataset(truth).stream()).toArray(String[]::new), env()).genConfig()))) {
            for (final EvalStatsMutations.CounterKey k : EvalStatsMutations.counterKeys(st)) {
                referenceCounters.add(BulkBuild.counterRead(k.row().getBytes(StandardCharsets.UTF_8), k.family().getBytes(StandardCharsets.UTF_8),
                        k.graph().getBytes(StandardCharsets.UTF_8)));
            }
        }
        final long sharedOnes = referenceCounters.stream()
                .filter(k -> truthReads.getOrDefault(k, 1L) - referenceOnly.getOrDefault(k, 0L) == 1).count();
        assertTrue(sharedOnes > 0, "no counter the reference data shares with exactly one statement");
        assertTrue(onesNextToAnotherLabel(truth + "eval") > 0, "no counter with a 1 next to another label's cell");

        for (final String prefix : List.of(loaded, built, whole, parts)) {
            for (final String table : List.of("spo", "po", "osp", "text", "geo", "entity")) {
                assertSame(prefix + table, cells(truth + table, true), cells(prefix + table, true));
            }
            assertSame(prefix + "graphs", cells(truth + "graphs", false), cells(prefix + "graphs", false));
            assertEquals(counters(truth + "textcount"), counters(prefix + "textcount"), prefix + "textcount");
            assertSameReads(truth + "eval", prefix + "eval");
        }
    }

    /** Every set of labels the tests' readers may hold. */
    private static final List<String[]> READERS = List.of(new String[0], new String[] {"A"}, new String[] {"B"}, AUTHS);

    /**
     * Two counter tables the planner reads alike, for every reader: a counter (row, family and
     * qualifier) is the sum of the label cells the reader can see, and reads as 1 when it sees none.
     * The datatype markers compare by presence: each write call adds its own +1.
     */
    private void assertSameReads(final String expectedTable, final String actualTable) {
        for (final String[] labels : READERS) {
            final Map<String, Long> want = reads(expectedTable, labels);
            final Map<String, Long> got = reads(actualTable, labels);
            final Set<String> keys = new TreeSet<>(want.keySet());
            keys.addAll(got.keySet());
            final Set<String> diff = new TreeSet<>();
            for (final String k : keys) {
                final long w = want.getOrDefault(k, 1L);
                final long g = got.getOrDefault(k, 1L);
                if (w != g) {
                    diff.add(readable(k) + " want " + w + " got " + g);
                }
            }
            if (!diff.isEmpty()) {
                fail(actualTable + " read with " + java.util.Arrays.toString(labels) + ": " + diff.size() + " counters differ, e.g. "
                        + diff.stream().limit(8).toList());
            }
        }
        assertEquals(markers(expectedTable), markers(actualTable), actualTable + ": datatype markers");
    }

    /** A counter table as read with {@code labels}: each counter's visible cells added up, markers aside. */
    private Map<String, Long> reads(final String table, final String[] labels) {
        final Map<String, Long> out = new HashMap<>();
        try (CellScanner cells = getStore().scan(table, KeyRange.all(), ScanSpec.of(Auths.of(labels)))) {
            while (cells.hasNext()) {
                final Cell c = cells.next();
                if (!new String(c.family(), StandardCharsets.UTF_8).equals(org.tripatha.store.DatatypeMarkers.FAMILY)) {
                    out.merge(BulkBuild.counterRead(c.row(), c.family(), c.qualifier()), getStore().counterValue(c.value()), Long::sum);
                }
            }
        }
        return out;
    }

    private Set<String> markers(final String table) {
        final Set<String> out = new TreeSet<>();
        try (CellScanner cells = getStore().scan(table, KeyRange.all(), ScanSpec.of(Auths.of(AUTHS)))) {
            while (cells.hasNext()) {
                final Cell c = cells.next();
                if (new String(c.family(), StandardCharsets.UTF_8).equals(org.tripatha.store.DatatypeMarkers.FAMILY)) {
                    out.add(key(c));
                }
            }
        }
        return out;
    }

    /** The counters with two label cells or more, one of them 1, in a family whose 1s may stay out. */
    private long onesNextToAnotherLabel(final String table) {
        final Map<String, List<Long>> byCounter = new HashMap<>();
        try (CellScanner cells = getStore().scan(table, KeyRange.all(), ScanSpec.of(Auths.of(AUTHS)))) {
            while (cells.hasNext()) {
                final Cell c = cells.next();
                final String family = new String(c.family(), StandardCharsets.UTF_8);
                if (EvalStatsMutations.COUNTER_FAMILIES.contains(family) && !family.equals(RdfCloudTripleStoreConstants.PRED_CF)) {
                    byCounter.computeIfAbsent(BulkBuild.counterRead(c.row(), c.family(), c.qualifier()), k -> new ArrayList<>())
                            .add(getStore().counterValue(c.value()));
                }
            }
        }
        return byCounter.values().stream().filter(v -> v.size() > 1 && v.contains(1L)).count();
    }

    private static String readable(final String counterRead) {
        final String[] p = counterRead.split(":", -1);
        final java.util.HexFormat hex = java.util.HexFormat.of();
        return (new String(hex.parseHex(p[0]), StandardCharsets.UTF_8) + " | " + new String(hex.parseHex(p[1]), StandardCharsets.UTF_8)
                + " | " + new String(hex.parseHex(p[2]), StandardCharsets.UTF_8)).replace('\u0000', '~');
    }

    // ------------------------------------------------------------------ files

    private static final String XSD = "http://www.w3.org/2001/XMLSchema#";

    /** Two files: blank nodes at both ends of one, the same label in the other, graphs, RDF 1.2, a repeat, a bad line. */
    private List<Path> rdfFiles() throws Exception {
        final StringBuilder a = new StringBuilder();
        a.append("_:b1 <urn:name> \"blank one\" .\n");
        a.append("<urn:s1> <urn:p> \"hello world\"@en <urn:g1> .\n");
        a.append("<urn:s1> <urn:age> \"42\"^^<").append(XSD).append("integer> .\n");
        a.append("<urn:s2> <urn:knows> _:b1 <urn:g2> .\n");
        a.append("# a comment\n\n");
        a.append("<urn:s2> <urn:when> \"2026-11-21T10:00:00Z\"^^<").append(XSD).append("dateTime> .\n");
        a.append("<urn:s3> <urn:says> <<( <urn:s1> <urn:p> \"x\" )>> .\n");
        a.append("<urn:s3> <urn:says> <<( _:b2 <urn:p> \"y\" )>> <urn:g1> .\n");
        a.append("<urn:s3> <urn:label> \"marhaban\"@ar--rtl .\n");
        a.append("this is not rdf\n");
        for (int i = 0; i < 400; i++) {
            a.append("<urn:e").append(i).append("> <urn:p").append(i % 5).append("> \"text number ").append(i % 37).append(" of many\"")
                    .append(i % 3 == 0 ? "" : " <urn:g" + i % 3 + ">").append(" .\n");
            a.append("<urn:e").append(i).append("> <urn:n> \"").append(i).append("\"^^<").append(XSD).append("int> .\n");
        }
        a.append("<urn:s1> <urn:p> \"hello world\"@en <urn:g1> .\n");
        a.append("_:b1 <urn:knows> <urn:s1> .\n");
        final Path f1 = dir.resolve("in/one.nq");
        Files.createDirectories(f1.getParent());
        Files.writeString(f1, a.toString(), StandardCharsets.UTF_8);
        final Path f2 = dir.resolve("in/sub/two.nt");
        Files.createDirectories(f2.getParent());
        Files.writeString(f2, "_:b1 <urn:name> \"blank one of file two\" .\n<urn:s1> <urn:p> \"other\" .\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("in/README.txt"), "not RDF", StandardCharsets.UTF_8);
        return List.of(f1, f2);
    }

    private String[] files(final String prefix, final String work, final String... extra) {
        final List<String> a = new ArrayList<>(List.of("files", "--table-prefix", prefix, "--input", dir.resolve("in").toString(),
                "--work-dir", dir.resolve(work).toString(), "--visibility", "A", "--text-index", "--input-splits", "8",
                "--file-size", "0.01"));
        a.addAll(List.of(extra));
        return a.toArray(new String[0]);
    }

    /** The statements a DAO load of the files writes, read whole with the job's timestamp and label. */
    private List<RyaStatement> statements(final List<Path> paths, final long jobTime) throws Exception {
        final List<RyaStatement> out = new ArrayList<>();
        for (final Path p : paths) {
            final org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(p.toUri());
            final FileStatus status = hp.getFileSystem(new Configuration()).globStatus(hp)[0];
            final RdfLines lines = new RdfLines(RdfLines.fileId(status), jobTime, "A".getBytes(StandardCharsets.UTF_8));
            for (final String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                try {
                    lines.parse(line, out);
                } catch (final RuntimeException e) {
                    assertEquals("this is not rdf", line);
                }
            }
        }
        return out;
    }

    private void daoLoad(final String prefix, final List<RyaStatement> statements) {
        final StoreRdfConfiguration conf = LoaderOptions.parse(new String[] {"build", "--table-prefix", prefix, "--text-index"}, env())
                .rdfConf();
        final RyaStoreDAO dao = new RyaStoreDAO(getStore(), conf);
        dao.init();
        try {
            dao.add(statements.iterator(), EvalStatsMutations.CounterPolicy.ALL);
        } finally {
            dao.destroy();
        }
    }

    private long jobTime(final String work) throws Exception {
        final Properties p = new Properties();
        try (var r = Files.newBufferedReader(dir.resolve(work).resolve("job.properties"))) {
            p.load(r);
        }
        return Long.parseLong(p.getProperty("timestamp"));
    }

    @Test
    void filesWriteWhatADaoLoadWrites() throws Exception {
        final List<Path> paths = rdfFiles();
        final String sparked = getTablePrefix();
        final String loaded = getTablePrefix() + "l_";
        final String out = spark(files(sparked, "work", "--max-bad-lines", "1"));
        assertTrue(out.contains("1 bad lines left out"), out);
        assertTrue(out.contains("Left out (not .nt or .nq)"), out);
        final List<RyaStatement> statements = statements(paths, jobTime("work"));
        daoLoad(loaded, statements);

        for (final String table : List.of("spo", "po", "osp", "text")) {
            assertSame(table, cells(loaded + table, true), cells(sparked + table, true));
        }
        assertSame("graphs", cells(loaded + "graphs", false), cells(sparked + "graphs", false));
        assertSame("textcount", cells(loaded + "textcount", false), cells(sparked + "textcount", false));
        assertSameReads(loaded + "eval", sparked + "eval");

        // _:b1 is one node across the splits of its file, and another one in the other file.
        final Set<String> blank = new TreeSet<>();
        for (final RyaStatement st : statements) {
            if (st.getSubject().getData().startsWith(RyaSchema.BNODE_NAMESPACE)) {
                blank.add(st.getSubject().getData());
            }
        }
        assertEquals(2, blank.size(), blank.toString());
        assertTrue(cells(sparked + "spo", false).size() > 800);

        // The new store was split and sized as prepare does it.
        assertEquals(10L << 30, getStore().tables().targetSize(sparked + "spo"));
        assertTrue(getStore().tables().splitCount(sparked + "spo") >= 3, "at least 4 tablets per server");
    }

    @Test
    void filesWithDistinctCountEachStatementOnce() throws Exception {
        final List<Path> paths = rdfFiles();
        final String sparked = getTablePrefix();
        final String loaded = getTablePrefix() + "l_";
        spark(files(sparked, "work", "--max-bad-lines", "1", "--distinct"));
        final List<RyaStatement> all = statements(paths, jobTime("work"));
        // Once each, as the store tells statements apart: by their spo cell.
        final RyaTripleContext context = RyaTripleContext.getInstance(new StoreRdfConfiguration());
        final Map<Tasks.Bytes, RyaStatement> distinct = new LinkedHashMap<>();
        for (final RyaStatement st : all) {
            distinct.putIfAbsent(Tasks.Bytes.of(context.serializeTriple(st).get(TABLE_LAYOUT.SPO)), st);
        }
        assertEquals(all.size() - 1, distinct.size(), "the files repeat one statement");
        daoLoad(loaded, new ArrayList<>(distinct.values()));
        for (final String table : List.of("spo", "po", "osp", "text")) {
            assertSame(table, cells(loaded + table, true), cells(sparked + table, true));
        }
        assertSameReads(loaded + "eval", sparked + "eval");
    }

    @Test
    void tooManyBadLinesStopTheWriteAndARunAgainStartsOver() throws Exception {
        rdfFiles();
        final String sparked = getTablePrefix();
        assertThrows(Exception.class, () -> spark(files(sparked, "work")));
        assertFalse(cells(sparked + "spo", false).iterator().hasNext(), "nothing imported");
        final String out = spark(files(sparked, "work", "--max-bad-lines", "1"));
        assertTrue(out.contains("Import: "), out);
        assertTrue(cells(sparked + "spo", false).size() > 800);
        // A store that holds statements takes more files only with --append.
        assertThrows(StoreCheck.Mismatch.class, () -> spark(files(sparked, "work2", "--max-bad-lines", "1")));
    }

    @Test
    void smokeWritesImportsAndReadsBackAFile() throws Exception {
        final String out = spark("smoke", "--table-prefix", getTablePrefix(), "--work-dir", dir.resolve("smoke-work").toString());
        assertTrue(out.contains("Smoke test passed"), out);
        assertTrue(out.contains("Java 25"), out);
        assertFalse(getStore().tables().exists(getTablePrefix() + "spark_smoke"));
    }

    // ------------------------------------------------------------------ cells

    /** Every cell of a table, under the labels, as text; with or without its timestamp. */
    private Set<String> cells(final String table, final boolean timestamps) {
        final Set<String> out = new HashSet<>();
        try (CellScanner cells = getStore().scan(table, KeyRange.all(), ScanSpec.of(Auths.of(AUTHS)))) {
            while (cells.hasNext()) {
                final Cell c = cells.next();
                out.add(key(c) + (timestamps ? " @" + c.timestamp() : "") + " = " + java.util.HexFormat.of().formatHex(c.value()));
            }
        }
        return out;
    }

    private static String key(final Cell c) {
        return new String(c.row(), StandardCharsets.UTF_8) + " | " + new String(c.family(), StandardCharsets.UTF_8) + " | "
                + new String(c.qualifier(), StandardCharsets.UTF_8) + " | " + new String(c.visibility(), StandardCharsets.UTF_8);
    }

    private Map<String, Long> counters(final String table) {
        final Map<String, Long> out = new HashMap<>();
        try (CellScanner cells = getStore().scan(table, KeyRange.all(), ScanSpec.of(Auths.of(AUTHS)))) {
            while (cells.hasNext()) {
                final Cell c = cells.next();
                out.merge(key(c), getStore().counterValue(c.value()), Long::sum);
            }
        }
        return out;
    }

    private static void assertSame(final String what, final Set<String> expected, final Set<String> actual) {
        if (expected.equals(actual)) {
            assertFalse(expected.isEmpty(), what + " is empty");
            return;
        }
        final Set<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        final Set<String> extra = new TreeSet<>(actual);
        extra.removeAll(expected);
        fail(what + ": " + missing.size() + " missing, e.g. " + missing.stream().limit(5).toList() + "; " + extra.size()
                + " unexpected, e.g. " + extra.stream().limit(5).toList());
    }
}
