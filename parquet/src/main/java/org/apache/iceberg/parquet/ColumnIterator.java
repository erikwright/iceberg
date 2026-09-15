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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;

public abstract class ColumnIterator<T> extends BaseColumnIterator implements TripleIterator<T> {
  @SuppressWarnings("unchecked")
  static <T> ColumnIterator<T> newIterator(ColumnDescriptor desc, String writerVersion) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ColumnIterator<T>)
            new ColumnIterator<Boolean>(desc, writerVersion) {
              @Override
              public Boolean next() {
                return nextBoolean();
              }
            };
      case INT32:
        return (ColumnIterator<T>)
            new ColumnIterator<Integer>(desc, writerVersion) {
              @Override
              public Integer next() {
                return nextInteger();
              }
            };
      case INT64:
        return (ColumnIterator<T>)
            new ColumnIterator<Long>(desc, writerVersion) {
              @Override
              public Long next() {
                return nextLong();
              }
            };
      case INT96:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      case FLOAT:
        return (ColumnIterator<T>)
            new ColumnIterator<Float>(desc, writerVersion) {
              @Override
              public Float next() {
                return nextFloat();
              }
            };
      case DOUBLE:
        return (ColumnIterator<T>)
            new ColumnIterator<Double>(desc, writerVersion) {
              @Override
              public Double next() {
                return nextDouble();
              }
            };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (ColumnIterator<T>)
            new ColumnIterator<Binary>(desc, writerVersion) {
              @Override
              public Binary next() {
                return nextBinary();
              }
            };
      default:
        throw new UnsupportedOperationException(
            "Unsupported primitive type: " + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  private final PageIterator<T> pageIterator;
  private ColumnReader synchronizedReader;
  private long remainingRows;

  void setPageSource(ColumnReaderPageStore store) {
    this.synchronizedReader = store.columnReader(desc);
    this.remainingRows = store.getRowCount();
  }

  @Override
  public void setPageSource(PageReader source) {
    this.synchronizedReader = null;
    super.setPageSource(source);
  }

  @Override
  public boolean hasNext() {
    return synchronizedReader == null ? super.hasNext() : remainingRows > 0;
  }

  private void consumeSynchronized() {
    synchronizedReader.consume();
    if (synchronizedReader.getCurrentRepetitionLevel() == 0) {
      this.remainingRows -= 1;
    }
  }

  private ColumnIterator(ColumnDescriptor desc, String writerVersion) {
    super(desc);
    this.pageIterator = PageIterator.newIterator(desc, writerVersion);
  }

  @Override
  public int currentDefinitionLevel() {
    if (synchronizedReader != null) {
      return synchronizedReader.getCurrentDefinitionLevel();
    }

    advance();
    return pageIterator.currentDefinitionLevel();
  }

  @Override
  public int currentRepetitionLevel() {
    if (synchronizedReader != null) {
      return synchronizedReader.getCurrentRepetitionLevel();
    }

    advance();
    return pageIterator.currentRepetitionLevel();
  }

  @Override
  public boolean nextBoolean() {
    if (synchronizedReader != null) {
      boolean value = synchronizedReader.getBoolean();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextBoolean();
  }

  @Override
  public int nextInteger() {
    if (synchronizedReader != null) {
      int value = synchronizedReader.getInteger();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextInteger();
  }

  @Override
  public long nextLong() {
    if (synchronizedReader != null) {
      long value = synchronizedReader.getLong();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextLong();
  }

  @Override
  public float nextFloat() {
    if (synchronizedReader != null) {
      float value = synchronizedReader.getFloat();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextFloat();
  }

  @Override
  public double nextDouble() {
    if (synchronizedReader != null) {
      double value = synchronizedReader.getDouble();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextDouble();
  }

  @Override
  public Binary nextBinary() {
    if (synchronizedReader != null) {
      Binary value = synchronizedReader.getBinary();
      consumeSynchronized();
      return value;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextBinary();
  }

  @Override
  public <N> N nextNull() {
    if (synchronizedReader != null) {
      consumeSynchronized();
      return null;
    }

    this.triplesRead += 1;
    advance();
    return pageIterator.nextNull();
  }

  @Override
  protected BasePageIterator pageIterator() {
    return pageIterator;
  }
}
