package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 19.02.13
 */
public class OLocalHashTableV3TestIT extends OLocalHashTableV3Base {
  private OrientDB orientDB;

  private static final String DB_NAME = "localHashTableTest";

  @Before
  public void before() {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    OFileUtils.deleteRecursively(dbDirectory);
    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());

    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);
    final ODatabaseSession databaseDocumentTx = orientDB.open(DB_NAME, "admin", "admin");

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTableV3<>("localHashTableTest", ".imc", ".tsc", ".obf", ".nbh",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());

    localHashTable
        .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().<String>getObjectSerializer(OType.STRING), null,
            true, null, murmurHash3HashFunction);

  }

  @After
  public void after() {
    orientDB.drop(DB_NAME);
    orientDB.close();
  }

}
