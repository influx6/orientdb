/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cluster.flatcluster;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 19.03.13
 */
public final class OClusterPage extends ODurablePage {
  private static final int MAX_RECORDS_COUNT    = 1 << 10;
  private static final int INDEX_MASK           = MAX_RECORDS_COUNT - 1;
  private static final int MAX_DELETION_COUNTER = (1 << 24) - 1;

  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int PREV_PAGE_OFFSET = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int FREELIST_HEADER_OFFSET     = PREV_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_POSITION_OFFSET       = FREELIST_HEADER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_SPACE_COUNTER_OFFSET  = FREE_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int ENTRIES_COUNT_OFFSET       = FREE_SPACE_COUNTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_LENGTH_OFFSET = ENTRIES_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_OFFSET        = PAGE_INDEXES_LENGTH_OFFSET + OIntegerSerializer.INT_SIZE;

  /**
   * Position of record inside the page (int) negative if that is tombstone (record is deleted) and refers to other tombstone of if
   * any,
   * version of record (int) value is negative if this record is part of other record,
   * deletion counter (int), link to next record (long) or 0 if that is single record.
   */
  private static final int INDEX_ITEM_SIZE = OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE;
  private static final int MIN_RECORD_SIZE = 64 - INDEX_ITEM_SIZE - 2 * OIntegerSerializer.INT_SIZE - OShortSerializer.SHORT_SIZE;

  public static final int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  private static final int MAX_ENTRY_SIZE  = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;
  public static final  int MAX_RECORD_SIZE = MAX_ENTRY_SIZE - 2 * OIntegerSerializer.INT_SIZE - OShortSerializer.SHORT_SIZE;

  private static final int MARKED_AS_DELETED_FLAG = 1 << 16;
  private static final int POSITION_MASK          = 0xFFFF;

  private static final int ENTRY_KIND_HOLE    = -1;
  private static final int ENTRY_KIND_UNKNOWN = 0;
  private static final int ENTRY_KIND_DATA    = +1;

  public OClusterPage(OCacheEntry cacheEntry, boolean newPage) {
    super(cacheEntry);

    if (newPage) {
      setLongValue(NEXT_PAGE_OFFSET, -1);
      setLongValue(PREV_PAGE_OFFSET, -1);

      setIntValue(FREELIST_HEADER_OFFSET, 0);
      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, 0);
      setIntValue(ENTRIES_COUNT_OFFSET, 0);

      setIntValue(FREE_POSITION_OFFSET, PAGE_SIZE);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, PAGE_SIZE - PAGE_INDEXES_OFFSET);
    }
  }

  public long appendRecord(final int recordVersion, final byte[] record) {
    int freePosition = getIntValue(FREE_POSITION_OFFSET);
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    //entry size(int) + index of the reference inside the header(short) + recordSize +  record itself.
    final int entrySize = calculateEntrySize(record);
    int entryIndex;
    int deletionCounter = 0;

    while (true) {
      final int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

      if (!checkSpace(entrySize, freeListHeader > 0)) {
        return -1;
      }

      if (freeListHeader > 0) {
        if (freePosition - entrySize < lastEntryIndexPosition) {
          doDefragmentation();
        }
      } else {
        if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE) {
          doDefragmentation();
        }
      }

      freePosition = getIntValue(FREE_POSITION_OFFSET);
      freePosition -= entrySize;

      if (freeListHeader > 0) {
        entryIndex = freeListHeader - 1;

        final int tombstonePointer = getIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * entryIndex);

        int nextEntryPosition = tombstonePointer & POSITION_MASK;
        if (nextEntryPosition > 0) {
          setIntValue(FREELIST_HEADER_OFFSET, nextEntryPosition);
        } else {
          setIntValue(FREELIST_HEADER_OFFSET, 0);
        }

        int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
        deletionCounter = getIntValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE);
        if (deletionCounter >= MAX_DELETION_COUNTER) {
          continue;
        }

        setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);
        setIntValue(entryIndexPosition, freePosition);
        setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);

        deletionCounter++;
        setIntValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE, deletionCounter);
        setLongValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE, 0);
      } else {
        entryIndex = indexesLength;

        setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
        setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

        int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
        setIntValue(entryIndexPosition, freePosition);
        setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
        setIntValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE, deletionCounter);
        setLongValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE, 0);
      }

      break;
    }

    int entryPosition = freePosition;
    setIntValue(entryPosition, entrySize);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setShortValue(entryPosition, (short) entryIndex);
    entryPosition += OShortSerializer.SHORT_SIZE;

    setIntValue(entryPosition, record.length);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setBinaryValue(entryPosition, record);

    setIntValue(FREE_POSITION_OFFSET, freePosition);

    incrementEntriesCount();

    return (((long) deletionCounter) << 10) | entryIndex;
  }

  public boolean replaceRecord(final long position, byte[] record, final int recordVersion) {
    final int entryIndex = (int) (position & INDEX_MASK);
    final int deletionCounter = (int) (position >>> 10);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
    int entryPosition = getIntValue(entryIndexPosition);
    final int currentDeletionCounter = getIntValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE);

    if (entryPosition <= 0 || currentDeletionCounter != deletionCounter) {
      throw new IllegalStateException("Record " + position + " does not exist in cluster");
    }

    final int currentEntrySize = getIntValue(entryPosition);
    assert currentEntrySize != 0;

    if (currentEntrySize < 0) {
      throw new IllegalStateException("Record " + position + " does not exist in cluster");
    }

    final int newEntrySize = calculateEntrySize(record);
    final int sizeDifference = newEntrySize - currentEntrySize;

    final int freeSpace = getFreeSpace();
    if (freeSpace - sizeDifference < 0) {
      return false;
    }

    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    int freePosition = getIntValue(FREE_POSITION_OFFSET);
    if (sizeDifference == 0) {
      int recordPosition = entryPosition + OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE;
      setIntValue(recordPosition, record.length);
      recordPosition += OIntegerSerializer.INT_SIZE;

      setBinaryValue(recordPosition, record);

      return true;
    } else {
      setIntValue(entryPosition, -currentEntrySize);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, freeSpace + currentEntrySize);
    }

    if (freePosition - newEntrySize < lastEntryIndexPosition) {
      doDefragmentation();
      freePosition = getIntValue(FREE_POSITION_OFFSET);
    }

    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - sizeDifference);

    freePosition -= newEntrySize;

    setIntValue(entryIndexPosition, freePosition);

    setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    setLongValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE, 0);

    entryPosition = freePosition;
    setIntValue(entryPosition, newEntrySize);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setShortValue(entryPosition, (short) entryIndex);
    entryPosition += OShortSerializer.SHORT_SIZE;

    setIntValue(entryPosition, record.length);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setBinaryValue(entryPosition, record);

    setIntValue(FREE_POSITION_OFFSET, freePosition);

    return true;
  }

  private int calculateEntrySize(final byte[] record) {
    return Math.max(record.length, MIN_RECORD_SIZE) + 2 * OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE;
  }

  public int getRecordVersion(final long position) {
    final int entryIndex = (int) (position & INDEX_MASK);
    final int deletionCounter = (int) (position >>> 10);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
    int entryPosition = getIntValue(entryIndexPosition);
    final int currentDeletionCounter = getIntValue(entryIndexPosition + 2 * OIntegerSerializer.INT_SIZE);

    if (entryPosition <= 0 || currentDeletionCounter != deletionCounter) {
      throw new IllegalStateException("Record " + position + " does not exist in cluster");
    }

    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    return getIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE);
  }

  public boolean isEmpty() {
    return getFreeSpace() == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private boolean checkSpace(int entrySize, boolean freeListHeader) {
    if (freeListHeader) {
      return getFreeSpace() - entrySize >= 0;
    }

    return getFreeSpace() - entrySize - INDEX_ITEM_SIZE >= 0;
  }

  public boolean deleteRecord(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return false;
    }

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return false;
    }

    int entryPosition = entryPointer & POSITION_MASK;

    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);
    if (freeListHeader <= 0) {
      setIntValue(entryIndexPosition, MARKED_AS_DELETED_FLAG);
    } else {
      setIntValue(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);
    }

    setIntValue(FREELIST_HEADER_OFFSET, position + 1);

    final int entrySize = getIntValue(entryPosition);
    assert entrySize > 0;

    setIntValue(entryPosition, -entrySize);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() + entrySize);

    decrementEntriesCount();

    return true;
  }

  public boolean isDeleted(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return true;
    }

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    return (entryPointer & MARKED_AS_DELETED_FLAG) != 0;
  }

  public int getRecordSize(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = getIntValue(entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }

    final int entryPosition = entryPointer & POSITION_MASK;
    return getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
  }

  public int findFirstDeletedRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
        return i;
      }
    }

    return -1;
  }

  public int findFirstRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  public int findLastRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    final int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  public final int getFreeSpace() {
    return getIntValue(FREE_SPACE_COUNTER_OFFSET);
  }

  public int getMaxRecordSize() {
    final int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    final int maxEntrySize;
    if (freeListHeader > 0) {
      maxEntrySize = getFreeSpace();
    } else {
      maxEntrySize = getFreeSpace() - INDEX_ITEM_SIZE;
    }

    final int result = maxEntrySize - 3 * OIntegerSerializer.INT_SIZE;
    if (result < 0) {
      return 0;
    }

    return result;
  }

  public final int getRecordsCount() {
    return getIntValue(ENTRIES_COUNT_OFFSET);
  }

  public long getNextPage() {
    return getLongValue(NEXT_PAGE_OFFSET);
  }

  public void setNextPage(final long nextPage) {
    setLongValue(NEXT_PAGE_OFFSET, nextPage);
  }

  public long getPrevPage() {
    return getLongValue(PREV_PAGE_OFFSET);
  }

  public void setPrevPage(final long prevPage) {
    setLongValue(PREV_PAGE_OFFSET, prevPage);
  }

  public void setRecordLongValue(final int recordPosition, final int offset, final long value) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      setLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, value);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      setLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, value);
    }
  }

  public long getRecordLongValue(final int recordPosition, final int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  public byte[] getRecordBinaryValue(final int recordPosition, final int offset, final int size) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, size);

      return getBinaryValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, size);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);

      return getBinaryValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, size);
    }
  }

  private boolean insideRecordBounds(final int entryPosition, final int offset, final int contentSize) {
    final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    return offset >= 0 && offset + contentSize <= recordSize;
  }

  private void incrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() + 1);
  }

  private void decrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() - 1);
  }

  private boolean isPositionInsideInterval(final int recordPosition) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    return recordPosition < indexesLength;
  }

  private void doDefragmentation() {
    final int recordsCount = getRecordsCount();
    final int freePosition = getIntValue(FREE_POSITION_OFFSET);

    // 1. Build the entries "map" and merge consecutive holes.

    final int maxEntries = recordsCount /* live records */ + recordsCount + 1 /* max holes after merging */;
    final int[] positions = new int[maxEntries];
    final int[] sizes = new int[maxEntries];

    int count = 0;
    int currentPosition = freePosition;
    int lastEntryKind = ENTRY_KIND_UNKNOWN;
    while (currentPosition < PAGE_SIZE) {
      final int size = getIntValue(currentPosition);
      final int entryKind = Integer.signum(size);
      assert entryKind != ENTRY_KIND_UNKNOWN;

      if (entryKind == ENTRY_KIND_HOLE && lastEntryKind == ENTRY_KIND_HOLE) {
        sizes[count - 1] += size;
      } else {
        positions[count] = currentPosition;
        sizes[count] = size;

        ++count;

        lastEntryKind = entryKind;
      }

      currentPosition += entryKind == ENTRY_KIND_HOLE ? -size : size;
    }

    // 2. Iterate entries in reverse, update data offsets, merge consecutive data segments and move them in a single operation.

    int shift = 0;
    int lastDataPosition = 0;
    int mergedDataSize = 0;
    for (int i = count - 1; i >= 0; --i) {
      final int position = positions[i];
      final int size = sizes[i];

      final int entryKind = Integer.signum(size);
      assert entryKind != ENTRY_KIND_UNKNOWN;

      if (entryKind == ENTRY_KIND_DATA && shift > 0) {
        final int positionIndex = getShortValue(position + OIntegerSerializer.INT_SIZE) & 0xFFFF;
        setIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * positionIndex, position + shift);

        lastDataPosition = position;
        mergedDataSize += size; // accumulate consecutive data segments size
      }

      if (mergedDataSize > 0 && (entryKind == ENTRY_KIND_HOLE || i == 0)) { // move consecutive merged data segments in one go
        moveData(lastDataPosition, lastDataPosition + shift, mergedDataSize);
        mergedDataSize = 0;
      }

      if (entryKind == ENTRY_KIND_HOLE) {
        shift += -size;
      }
    }

    // 3. Update free position.

    setIntValue(FREE_POSITION_OFFSET, freePosition + shift);
  }
}