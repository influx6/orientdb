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

package com.orientechnologies.orient.core.storage.index.hashindex.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OHashTableDirectoryException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/14/14
 */
public class OHashTableDirectory extends ODurableComponent {
  static final int ITEM_SIZE = OLongSerializer.LONG_SIZE;

  private static final int LEVEL_SIZE = OLocalHashTable20.MAX_LEVEL_SIZE;

  static final int BINARY_LEVEL_SIZE = LEVEL_SIZE * ITEM_SIZE + 3 * OByteSerializer.BYTE_SIZE;

  private long fileId;

  private final long firstEntryIndex;

  OHashTableDirectory(String defaultExtension, String name, String lockName, OAbstractPaginatedStorage storage) {
    super(storage, name, defaultExtension, lockName);
    this.firstEntryIndex = 0;
  }

  public void create() throws IOException {
    acquireExclusiveLock();
    try {
      fileId = addFile(getFullName());
      init();
    } finally {
      releaseExclusiveLock();
    }
  }

  private void init() throws IOException {
    startAtomicOperation(false);
    try {
      OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);

      if (firstEntry == null) {
        firstEntry = addPage(fileId);
        assert firstEntry.getPageIndex() == 0;
      }

      pinPage(firstEntry);

      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);

        firstPage.setTreeSize(0);
        firstPage.setTombstone(-1);

      } finally {
        releasePageFromWrite(firstEntry);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during hash table initialization", this), e);
    }
  }

  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      fileId = openFile(getFullName());
      final int filledUpTo = (int) getFilledUpTo(fileId);

      for (int i = 0; i < filledUpTo; i++) {
        final OCacheEntry entry = loadPageForRead(fileId, i, true);
        assert entry != null;

        pinPage(entry);
        releasePageFromRead(entry);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  public void close() throws IOException {
    acquireExclusiveLock();
    try {
      readCache.closeFile(fileId, true, writeCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() throws IOException {
    acquireExclusiveLock();
    try {
      deleteFile(fileId);
    } finally {
      releaseExclusiveLock();
    }
  }

  void deleteWithoutOpen() throws IOException {
    acquireExclusiveLock();
    try {
      if (isFileExists(getFullName())) {
        fileId = openFile(getFullName());
        deleteFile(fileId);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  int addNewNode(byte maxLeftChildDepth, byte maxRightChildDepth, byte nodeLocalDepth, long[] newNode) throws IOException {
    int nodeIndex;

    startAtomicOperation(true);
    acquireExclusiveLock();
    try {
      OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);
      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);

        final int tombstone = firstPage.getTombstone();

        if (tombstone >= 0)
          nodeIndex = tombstone;
        else {
          nodeIndex = firstPage.getTreeSize();
          firstPage.setTreeSize(nodeIndex + 1);
        }

        if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
          final int localNodeIndex = nodeIndex;

          firstPage.setMaxLeftChildDepth(localNodeIndex, maxLeftChildDepth);
          firstPage.setMaxRightChildDepth(localNodeIndex, maxRightChildDepth);
          firstPage.setNodeLocalDepth(localNodeIndex, nodeLocalDepth);

          if (tombstone >= 0)
            firstPage.setTombstone((int) firstPage.getPointer(nodeIndex, 0));

          for (int i = 0; i < newNode.length; i++)
            firstPage.setPointer(localNodeIndex, i, newNode[i]);

        } else {
          final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;
          final int localLevel = nodeIndex % ODirectoryPage.NODES_PER_PAGE;

          OCacheEntry cacheEntry = loadPageForWrite(fileId, pageIndex, true);
          while (cacheEntry == null || cacheEntry.getPageIndex() < pageIndex) {
            if (cacheEntry != null)
              releasePageFromWrite(cacheEntry);

            cacheEntry = addPage(fileId);
          }

          try {
            ODirectoryPage page = new ODirectoryPage(cacheEntry, cacheEntry);

            page.setMaxLeftChildDepth(localLevel, maxLeftChildDepth);
            page.setMaxRightChildDepth(localLevel, maxRightChildDepth);
            page.setNodeLocalDepth(localLevel, nodeLocalDepth);

            if (tombstone >= 0)
              firstPage.setTombstone((int) page.getPointer(localLevel, 0));

            for (int i = 0; i < newNode.length; i++)
              page.setPointer(localLevel, i, newNode[i]);

          } finally {
            releasePageFromWrite(cacheEntry);
          }
        }

      } finally {
        releasePageFromWrite(firstEntry);
      }

      endAtomicOperation(false, null);

    } catch (RuntimeException e) {
      endAtomicOperation(true, e);
      throw e;
    } finally {
      releaseExclusiveLock();
    }

    return nodeIndex;
  }

  void deleteNode(int nodeIndex) throws IOException {

    startAtomicOperation(true);
    acquireExclusiveLock();
    try {
      OCacheEntry firstEntry = loadPageForWrite(fileId, firstEntryIndex, true);
      try {
        ODirectoryFirstPage firstPage = new ODirectoryFirstPage(firstEntry, firstEntry);
        if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
          firstPage.setPointer(nodeIndex, 0, firstPage.getTombstone());
          firstPage.setTombstone(nodeIndex);
        } else {
          final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;
          final int localNodeIndex = nodeIndex % ODirectoryPage.NODES_PER_PAGE;

          final OCacheEntry cacheEntry = loadPageForWrite(fileId, pageIndex, true);
          try {
            ODirectoryPage page = new ODirectoryPage(cacheEntry, cacheEntry);

            page.setPointer(localNodeIndex, 0, firstPage.getTombstone());
            firstPage.setTombstone(nodeIndex);

          } finally {
            releasePageFromWrite(cacheEntry);
          }
        }
      } finally {
        releasePageFromWrite(firstEntry);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during node deletion", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  byte getMaxLeftChildDepth(int nodeIndex) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, false);
        try {
          return page.getMaxLeftChildDepth(getLocalNodeIndex(nodeIndex));
        } finally {
          releasePage(page, false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  void setMaxLeftChildDepth(int nodeIndex, byte maxLeftChildDepth) throws IOException {
    startAtomicOperation(true);
    acquireExclusiveLock();
    try {

      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setMaxLeftChildDepth(getLocalNodeIndex(nodeIndex), maxLeftChildDepth);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during setting of max left child depth", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  byte getMaxRightChildDepth(int nodeIndex) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, false);
        try {
          return page.getMaxRightChildDepth(getLocalNodeIndex(nodeIndex));
        } finally {
          releasePage(page, false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  void setMaxRightChildDepth(int nodeIndex, byte maxRightChildDepth) throws IOException {
    startAtomicOperation(true);
    acquireExclusiveLock();
    try {

      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setMaxRightChildDepth(getLocalNodeIndex(nodeIndex), maxRightChildDepth);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during setting of right max child depth", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  byte getNodeLocalDepth(int nodeIndex) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, false);
        try {
          return page.getNodeLocalDepth(getLocalNodeIndex(nodeIndex));
        } finally {
          releasePage(page, false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  void setNodeLocalDepth(int nodeIndex, byte localNodeDepth) throws IOException {
    startAtomicOperation(true);
    acquireExclusiveLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setNodeLocalDepth(getLocalNodeIndex(nodeIndex), localNodeDepth);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during setting of local node depth", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  long[] getNode(int nodeIndex) throws IOException {
    final long[] node = new long[LEVEL_SIZE];

    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, false);
        try {
          final int localNodeIndex = getLocalNodeIndex(nodeIndex);
          for (int i = 0; i < LEVEL_SIZE; i++)
            node[i] = page.getPointer(localNodeIndex, i);
        } finally {
          releasePage(page, false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }

    return node;
  }

  void setNode(int nodeIndex, long[] node) throws IOException {
    startAtomicOperation(true);
    acquireExclusiveLock();
    try {

      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        final int localNodeIndex = getLocalNodeIndex(nodeIndex);
        for (int i = 0; i < LEVEL_SIZE; i++)
          page.setPointer(localNodeIndex, i, node[i]);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during setting of node", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  long getNodePointer(int nodeIndex, int index) throws IOException {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        final ODirectoryPage page = loadPage(nodeIndex, false);
        try {
          return page.getPointer(getLocalNodeIndex(nodeIndex), index);
        } finally {
          releasePage(page, false);
        }
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  void setNodePointer(int nodeIndex, int index, long pointer) throws IOException {
    startAtomicOperation(true);
    acquireExclusiveLock();
    try {
      final ODirectoryPage page = loadPage(nodeIndex, true);
      try {
        page.setPointer(getLocalNodeIndex(nodeIndex), index, pointer);
      } finally {
        releasePage(page, true);
      }

      endAtomicOperation(false, null);
    } catch (IOException e) {
      endAtomicOperation(true, e);
      throw e;
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw OException.wrapException(new OHashTableDirectoryException("Error during setting of node pointer", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void clear() throws IOException {
    acquireExclusiveLock();
    try {
      truncateFile(fileId);
      init();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void flush() {
    atomicOperationsManager.acquireReadLock(this);
    try {
      acquireSharedLock();
      try {
        writeCache.flush(fileId);
      } finally {
        releaseSharedLock();
      }
    } finally {
      atomicOperationsManager.releaseReadLock(this);
    }
  }

  private ODirectoryPage loadPage(int nodeIndex, boolean exclusiveLock) throws IOException {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE) {
      OCacheEntry cacheEntry;

      if (exclusiveLock)
        cacheEntry = loadPageForWrite(fileId, firstEntryIndex, true);
      else
        cacheEntry = loadPageForRead(fileId, firstEntryIndex, true);

      return new ODirectoryFirstPage(cacheEntry, cacheEntry);
    }

    final int pageIndex = nodeIndex / ODirectoryPage.NODES_PER_PAGE;

    final OCacheEntry cacheEntry;

    if (exclusiveLock)
      cacheEntry = loadPageForWrite(fileId, pageIndex, true);
    else
      cacheEntry = loadPageForRead(fileId, pageIndex, true);

    return new ODirectoryPage(cacheEntry, cacheEntry);
  }

  private void releasePage(ODirectoryPage page, boolean exclusiveLock) {
    final OCacheEntry cacheEntry = page.getEntry();

    if (exclusiveLock)
      releasePageFromWrite(cacheEntry);
    else
      releasePageFromRead(cacheEntry);

  }

  private int getLocalNodeIndex(int nodeIndex) {
    if (nodeIndex < ODirectoryFirstPage.NODES_PER_PAGE)
      return nodeIndex;

    return (nodeIndex - ODirectoryFirstPage.NODES_PER_PAGE) % ODirectoryPage.NODES_PER_PAGE;
  }
}
