package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;
import org.junit.After;
import org.junit.Before;

import java.io.File;

public class OLocalHashTableV3EncryptionTestIT extends OLocalHashTableV3Base {
  private OrientDB orientDB;

  private static final String DB_NAME = "localHashTableEncryptionTest";

  @Before
  public void before() {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    OFileUtils.deleteRecursively(dbDirectory);
    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    ODatabaseSession databaseDocumentTx = orientDB.open(DB_NAME, "admin", "admin");

    final OEncryption encryption = OEncryptionFactory.INSTANCE.getEncryption("aes/gcm", "T1JJRU5UREJfSVNfQ09PTA==");

    OSHA256HashFunction<Integer> SHA256HashFunction = new OSHA256HashFunction<>(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTableV3<>("localHashTableEncryptionTest", ".imc", ".tsc", ".obf", ".nbh",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());

    localHashTable
        .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING), encryption,
            true, null, SHA256HashFunction);

  }

  @After
  public void after() {
    orientDB.drop(DB_NAME);
    orientDB.close();
  }
}
