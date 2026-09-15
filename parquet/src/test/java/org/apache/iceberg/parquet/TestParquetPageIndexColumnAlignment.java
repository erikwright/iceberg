/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies complete records when page pruning leaves nested columns on different page boundaries.
 */
class TestParquetPageIndexColumnAlignment {
  private static final int ROW_COUNT = 5000;
  private static final long FIRST_MATCH = 1234L;
  private static final long SECOND_MATCH = 3456L;
  private static final Types.StructType ELEMENT =
      Types.StructType.of(
          Types.NestedField.required(4, "name", Types.StringType.get()),
          Types.NestedField.optional(5, "value", Types.StringType.get()),
          Types.NestedField.optional(6, "number", Types.LongType.get()));
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "items", Types.ListType.ofOptional(3, ELEMENT)));
  private static final Schema PROJECTION =
      TypeUtil.join(SCHEMA, new Schema(MetadataColumns.ROW_POSITION));

  @TempDir private Path temp;

  @ParameterizedTest
  @ValueSource(strings = {"v1", "v2"})
  void alignsNestedValuesAcrossDisjointPages(String pageVersion) throws IOException {
    Path file = temp.resolve("nested.parquet");
    List<Record> expected = records();
    write(file, expected, pageVersion, 1000, 64 * 1024 * 1024);

    List<Long> candidates = Lists.newArrayList();
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file.toFile())))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData group = reader.getRowGroups().get(0);
      OffsetIndex idIndex = offsetIndex(reader, group, "id");
      OffsetIndex numberIndex = offsetIndex(reader, group, "items.list.element.number");
      // The custom reader returns page candidates, not exact predicate matches. Since id equals
      // physical position, the offset index identifies every row in the retained id pages.
      for (int page = 0; page < idIndex.getPageCount(); page += 1) {
        long first = idIndex.getFirstRowIndex(page);
        long last = idIndex.getLastRowIndex(page, group.getRowCount());
        if ((first <= FIRST_MATCH && FIRST_MATCH <= last)
            || (first <= SECOND_MATCH && SECOND_MATCH <= last)) {
          LongStream.rangeClosed(first, last).forEach(candidates::add);
        }
      }
      assertThat(candidates).contains(FIRST_MATCH, SECOND_MATCH).hasSizeLessThan(ROW_COUNT);
      long firstCandidate = candidates.get(0);
      assertThat(
              IntStream.range(0, numberIndex.getPageCount())
                  .anyMatch(
                      page ->
                          numberIndex.getFirstRowIndex(page) < firstCandidate
                              && numberIndex.getLastRowIndex(page, group.getRowCount())
                                  >= firstCandidate))
          .as("A nested number page must straddle the first candidate, requiring prefix skipping")
          .isTrue();
    }
    assertThat(candidates.get(candidates.size() - 1) - candidates.get(0) + 1)
        .as("The retained id pages must leave a gap between candidate ranges")
        .isGreaterThan(candidates.size());
    // With the same predicate but page pruning disabled, this single row group remains intact.
    assertRead(file, expected, LongStream.range(0, ROW_COUNT).boxed().toList(), false);
    assertRead(file, expected, candidates, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", "v2"})
  void resetsBetweenNativeAndSynchronizedGroups(String pageVersion) throws IOException {
    Path file = temp.resolve("multiple-groups.parquet");
    List<Record> expected = records();
    write(file, expected, pageVersion, 100, 32 * 1024);

    long firstGroupEnd;
    long thirdGroupStart;
    long middlePageStart;
    long middlePageEnd;
    Schema filterSchema;
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file.toFile())))) {
      assertThat(reader.getRowGroups().size()).isGreaterThanOrEqualTo(3);
      filterSchema = ParquetSchemaUtil.convert(reader.getFileMetaData().getSchema());
      BlockMetaData middle = reader.getRowGroups().get(1);
      firstGroupEnd = middle.getRowIndexOffset();
      thirdGroupStart = reader.getRowGroups().get(2).getRowIndexOffset();
      OffsetIndex index = offsetIndex(reader, middle, "id");
      assertThat(index.getPageCount())
          .as("Middle group must permit partial page selection")
          .isGreaterThan(2);
      middlePageStart = firstGroupEnd + index.getFirstRowIndex(1);
      middlePageEnd = firstGroupEnd + index.getLastRowIndex(1, middle.getRowCount()) + 1;
    }
    // Retain the first/last groups in full and one interior page of the middle group, so one
    // reader model switches native -> synchronized -> native while reusing record containers.
    Expression filter =
        Expressions.or(
            Expressions.lessThan("id", firstGroupEnd),
            Expressions.or(
                Expressions.equal("id", middlePageStart),
                Expressions.greaterThanOrEqual("id", thirdGroupStart)));
    // Fully retained groups use an unfiltered store. Pin that behavior so this cannot pass
    // without exercising both native/synchronized transitions.
    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(ParquetFilters.convert(filterSchema, filter, true))
            .build();
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file.toFile())), options)) {
      for (int group = 0; group < 3; group += 1) {
        try (PageReadStore pages = reader.readFilteredRowGroup(group)) {
          assertThat(pages).isNotNull();
          assertThat(pages.getRowIndexes().isPresent())
              .as("Only the middle row group must require synchronization")
              .isEqualTo(group == 1);
        }
      }
    }
    List<Long> candidates = Lists.newArrayList();
    LongStream.range(0, firstGroupEnd).forEach(candidates::add);
    LongStream.range(middlePageStart, middlePageEnd).forEach(candidates::add);
    LongStream.range(thirdGroupStart, ROW_COUNT).forEach(candidates::add);
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file.toFile()))
            .project(PROJECTION)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .reuseContainers()
            .enablePageIndexFilteringForPoc();
    assertRows(builder, expected, candidates);
  }

  private static void assertRead(
      Path file, List<Record> expected, List<Long> positions, boolean pageFiltering)
      throws IOException {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file.toFile()))
            .project(PROJECTION)
            .filter(
                Expressions.or(
                    Expressions.equal("id", FIRST_MATCH), Expressions.equal("id", SECOND_MATCH)))
            .createReaderFunc(GenericParquetReaders::buildReader);
    if (pageFiltering) {
      builder.enablePageIndexFilteringForPoc();
    }
    assertRows(builder, expected, positions);
  }

  private static void assertRows(
      Parquet.ReadBuilder builder, List<Record> expected, List<Long> positions) throws IOException {
    int count = 0;
    try (CloseableIterable<Record> rows = builder.build()) {
      for (Record row : rows) {
        assertThat(count).as("No extra candidate records").isLessThan(positions.size());
        long position = positions.get(count);
        count += 1;
        assertThat(row.getField(MetadataColumns.ROW_POSITION.name())).isEqualTo(position);
        assertThat(row.getField("id")).isEqualTo(position);
        assertThat(row.getField("items"))
            .as("Nested values at physical row %s", position)
            .isEqualTo(expected.get(Math.toIntExact(position)).getField("items"));
      }
    }
    assertThat(count).as("Every candidate record must be emitted").isEqualTo(positions.size());
  }

  private static OffsetIndex offsetIndex(ParquetFileReader reader, BlockMetaData group, String path)
      throws IOException {
    ColumnChunkMetaData column =
        group.getColumns().stream()
            .filter(chunk -> chunk.getPath().toDotString().equals(path))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Cannot find column: " + path));
    OffsetIndex index = reader.readOffsetIndex(column);
    assertThat(index).as("Offset index for %s", path).isNotNull();
    return index;
  }

  private static List<Record> records() {
    List<Record> records = Lists.newArrayList();
    for (int id = 0; id < ROW_COUNT; id += 1) {
      records.add(record(id));
    }
    return records;
  }

  private static void write(
      Path file, List<Record> records, String pageVersion, int pageRowLimit, long rowGroupBytes)
      throws IOException {
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file.toFile()))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_PAGE_VERSION, pageVersion)
            .set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "16384")
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, Integer.toString(pageRowLimit))
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupBytes))
            .withDictionaryEncoding("id", false)
            .build()) {
      writer.addAll(records);
    }
  }

  private static Record record(long id) {
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    List<Record> items = Lists.newArrayList();
    // Occasional wide lists force different leaf page boundaries; null lists, elements and
    // fields also require definition levels to stay aligned with the selected physical rows.
    int count = id % 97 == 0 ? 137 : (int) (id % 5);
    for (int index = 0; index < count; index += 1) {
      GenericRecord item = GenericRecord.create(ELEMENT);
      item.setField("name", index % 2 == 0 ? "first" : "second");
      item.setField("value", index % 3 == 0 ? null : "value-" + id + "-" + index);
      item.setField("number", index % 4 == 0 ? null : id * 1000 + index);
      items.add(index % 11 == 10 ? null : item);
    }
    row.setField("items", id % 19 == 0 ? null : items);
    return row;
  }
}
