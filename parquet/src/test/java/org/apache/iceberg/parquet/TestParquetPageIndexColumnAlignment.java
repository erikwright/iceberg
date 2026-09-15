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
import java.util.stream.LongStream;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestParquetPageIndexColumnAlignment {
  private static final int ROW_COUNT = 5000;
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

  @TempDir Path temp;

  @ParameterizedTest
  @ValueSource(strings = {"v1", "v2"})
  void alignsNestedValuesAcrossDisjointPages(String pageVersion) throws IOException {
    Path file = temp.resolve("nested.parquet");
    List<Record> expected = Lists.newArrayList();
    for (int id = 0; id < ROW_COUNT; id++) {
      expected.add(record(id));
    }
    try (FileAppender<Record> writer =
        Parquet.write(Files.localOutput(file.toFile()))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_PAGE_VERSION, pageVersion)
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, "16384")
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "1000")
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "67108864")
            .withDictionaryEncoding("id", false)
            .build()) {
      writer.addAll(expected);
    }

    List<Long> candidates = Lists.newArrayList();
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(Files.localInput(file.toFile())))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      BlockMetaData group = reader.getRowGroups().get(0);
      OffsetIndex idIndex = reader.readOffsetIndex(group.getColumns().get(0));
      OffsetIndex valueIndex = reader.readOffsetIndex(group.getColumns().get(2));
      assertThat(idIndex).isNotNull();
      assertThat(valueIndex).isNotNull();
      assertThat(valueIndex.getPageCount()).isGreaterThan(idIndex.getPageCount());
      for (int page = 0; page < idIndex.getPageCount(); page++) {
        long first = idIndex.getFirstRowIndex(page);
        long last = idIndex.getLastRowIndex(page, group.getRowCount());
        if ((first <= 1234 && 1234 <= last) || (first <= 3456 && 3456 <= last)) {
          LongStream.rangeClosed(first, last).forEach(candidates::add);
        }
      }
    }
    assertThat(candidates).contains(1234L, 3456L).hasSizeLessThan(ROW_COUNT);
    assertRead(file, expected, LongStream.range(0, ROW_COUNT).boxed().toList(), false);
    assertRead(file, expected, candidates, true);
  }

  private static void assertRead(
      Path file, List<Record> expected, List<Long> positions, boolean pageFiltering)
      throws IOException {
    Parquet.ReadBuilder builder =
        Parquet.read(Files.localInput(file.toFile()))
            .project(PROJECTION)
            .filter(Expressions.or(Expressions.equal("id", 1234L), Expressions.equal("id", 3456L)))
            .createReaderFunc(GenericParquetReaders::buildReader);
    if (pageFiltering) {
      builder.enablePageIndexFilteringForPoc();
    }
    int count = 0;
    try (CloseableIterable<Record> rows = builder.build()) {
      for (Record row : rows) {
        assertThat(count).as("No extra candidate records").isLessThan(positions.size());
        long position = positions.get(count++);
        assertThat(row.getField("_pos")).isEqualTo(position);
        assertThat(row.getField("id")).isEqualTo(position);
        assertThat(row.getField("items"))
            .as("Nested values at physical row %s", position)
            .isEqualTo(expected.get(Math.toIntExact(position)).getField("items"));
      }
    }
    assertThat(count).as("Every candidate record must be emitted").isEqualTo(positions.size());
  }

  private static Record record(long id) {
    GenericRecord row = GenericRecord.create(SCHEMA);
    row.setField("id", id);
    List<Record> items = Lists.newArrayList();
    int count = id % 97 == 0 ? 137 : (int) (id % 5);
    for (int index = 0; index < count; index++) {
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
