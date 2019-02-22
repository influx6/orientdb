package com.orientechnologies.orient.core.storage.cluster.flatcluster;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedClusterDebug;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterBrowsePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.File;
import java.io.IOException;

public class OFlatCluster extends OPaginatedCluster {
  private long fileId;

  public OFlatCluster(final OAbstractPaginatedStorage storage, final String name, final String extension, final String lockName) {
    super(storage, name, extension, lockName);
  }

  @Override
  public void replaceFile(final File file) throws IOException {
  }

  @Override
  public void replaceClusterMapFile(final File file) throws IOException {
  }

  @Override
  public RECORD_STATUS getRecordStatus(final long clusterPosition) throws IOException {
    return null;
  }

  @Override
  public OPaginatedClusterDebug readDebug(final long clusterPosition) throws IOException {
    return null;
  }

  @Override
  public void registerInStorageConfig(final OClusterBasedStorageConfiguration root) {

  }

  @Override
  public long getFileId() {
    return 0;
  }

  @Override
  public void configure(final OStorage iStorage, final int iId, final String iClusterName, final Object... iParameters)
      throws IOException {

  }

  @Override
  public void configure(final OStorage iStorage, final OStorageClusterConfiguration iConfig) throws IOException {

  }

  @Override
  public void create(final int iStartSize) throws IOException {

  }

  @Override
  public void open() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void close(final boolean flush) throws IOException {

  }

  @Override
  public void delete() throws IOException {

  }

  @Override
  public Object set(final ATTRIBUTES iAttribute, final Object iValue) throws IOException {
    return null;
  }

  @Override
  public String encryption() {
    return null;
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public void truncate() throws IOException {

  }

  @Override
  public OPhysicalPosition allocatePosition(final byte recordType) throws IOException {
    return null;
  }

  @Override
  public OPhysicalPosition createRecord(final byte[] content, final int recordVersion, final byte recordType,
      final OPhysicalPosition allocatedPosition) throws IOException {
    return null;
  }

  @Override
  public boolean deleteRecord(final long clusterPosition) throws IOException {
    return false;
  }

  @Override
  public void updateRecord(final long clusterPosition, final byte[] content, final int recordVersion, final byte recordType)
      throws IOException {

  }

  @Override
  public void recycleRecord(final long clusterPosition, final byte[] content, final int recordVersion, final byte recordType)
      throws IOException {

  }

  @Override
  public ORawBuffer readRecord(final long clusterPosition, final boolean prefetchRecords) throws IOException {
    return null;
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(final long clusterPosition, final int recordVersion)
      throws IOException, ORecordNotFoundException {
    return null;
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(final OPhysicalPosition iPPosition) throws IOException {
    return null;
  }

  @Override
  public boolean isDeleted(final OPhysicalPosition iPPosition) throws IOException {
    return false;
  }

  @Override
  public long getEntries() {
    return 0;
  }

  @Override
  public long getFirstPosition() throws IOException {
    return 0;
  }

  @Override
  public long getLastPosition() throws IOException {
    return 0;
  }

  @Override
  public long getNextPosition() throws IOException {
    return 0;
  }

  @Override
  public String getFileName() {
    return null;
  }

  @Override
  public int getId() {
    return 0;
  }

  @Override
  public void synch() throws IOException {

  }

  @Override
  public long getRecordsSize() throws IOException {
    return 0;
  }

  @Override
  public float recordGrowFactor() {
    return 0;
  }

  @Override
  public String compression() {
    return null;
  }

  @Override
  public float recordOverflowGrowFactor() {
    return 0;
  }

  @Override
  public boolean isSystemCluster() {
    return false;
  }

  @Override
  public OPhysicalPosition[] higherPositions(final OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(final OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] lowerPositions(final OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] floorPositions(final OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public boolean hideRecord(final long position) throws IOException {
    return false;
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return null;
  }

  @Override
  public void acquireAtomicExclusiveLock() {

  }

  @Override
  public OClusterBrowsePage nextPage(final long lastPosition) throws IOException {
    return null;
  }

  @Override
  public int getBinaryVersion() {
    return 0;
  }
}
