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

import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Adapts Parquet's synchronized column readers to Iceberg's record construction. */
final class ColumnReaderPageStore implements PageReadStore {
  private final PageReadStore delegate;
  private final ColumnReadStoreImpl columns;

  ColumnReaderPageStore(PageReadStore delegate, MessageType schema, String createdBy) {
    this.delegate = delegate;
    this.columns = new ColumnReadStoreImpl(delegate, converters(schema), schema, createdBy);
  }

  ColumnReader columnReader(ColumnDescriptor descriptor) {
    return columns.getColumnReader(descriptor);
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor descriptor) {
    return delegate.getPageReader(descriptor);
  }

  @Override
  public long getRowCount() {
    return delegate.getRowCount();
  }

  @Override
  public Optional<Long> getRowIndexOffset() {
    return delegate.getRowIndexOffset();
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return delegate.getRowIndexes();
  }

  @Override
  public void close() {
    delegate.close();
  }

  private static GroupConverter converters(GroupType group) {
    Converter[] children = new Converter[group.getFieldCount()];
    for (int i = 0; i < children.length; i++) {
      Type type = group.getType(i);
      children[i] =
          type.isPrimitive() ? new PrimitiveConverter() {} : converters(type.asGroupType());
    }

    return new GroupConverter() {
      @Override
      public Converter getConverter(int index) {
        return children[index];
      }

      @Override
      public void start() {}

      @Override
      public void end() {}
    };
  }
}
