package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;

import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.IllegalFormatException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.DBIterator;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import org.apache.commons.lang.SerializationUtils;

public class LevelDBClient extends com.yahoo.ycsb.DB {

  private DB db = null;
  private String format = null;
  private Set<String> tombStoneKeys = new HashSet<String>();
  private boolean dumpIt = false;
  private boolean allRows = false;
  private int numCols = 0;
  private static boolean dbg = false;
  private static int[] fieldArrayIdx;
  private static String DELIMITER = ":";
  private static String TOMBSTONE = "{TOMBSTONE}";

  private String concat (String key1, String key2) {
	return key1 + DELIMITER + key2 + DELIMITER;
  }

  /* Fisher-Yates shuffle */
  private static void fyShuffle(int array[], int size) {
    int     i, j, temp;
    Random rand = new Random();
    for (i = size - 1; i >= 1; i--) {
        j = rand.nextInt(i);
        temp = array[j];
        array[j] = array[i - 1];
        array[i - 1] = temp;
    }
  }

  private static void DEBUG (String format, Object... arguments) {
      if (dbg) System.out.format(format, arguments);
  }

  private static byte[] __serialize__(HashMap<String, ByteIterator> hash)
    throws IOException {

    List<String> list = new ArrayList<String>();

    for (Map.Entry<String, ByteIterator> entry : hash.entrySet()) {
        String key = entry.getKey();
        String val = entry.getValue().toString();
        String element = key + "CAFEBABE" + val;
        DEBUG("SERIALIZE. ELEMENT: %s\n", element);
        list.add(element);
    }

    /* FIXME: Can be optimized */
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutput out = new ObjectOutputStream(byteOut);
    out.writeObject(list);
    out.close();
    return byteOut.toByteArray();
  }

  private static
        HashMap<String, ByteIterator> __deserialize__(byte[] bytes)
        throws IOException, ClassNotFoundException {

    HashMap<String, ByteIterator> hash = new HashMap();
/*
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    ObjectInputStream in = new ObjectInputStream(byteIn);
    List<String> list = (List) in.readObject();
    in.close();
*/
    /* FIXME: This is takes huge time, appox 20000usec */
    List<String> list = (List) SerializationUtils.deserialize(bytes);
    for (String element: list) {
        String[] f = element.split("CAFEBABE");
        hash.put(f[0], new ByteArrayByteIterator(bytes(f[1])));
    }

    if (dbg) {
        for (Map.Entry<String, ByteIterator> entry : hash.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue().toString();
            DEBUG("DESERIALIZE. KEY: %s, VAL: %s\n", key, val);
        }
    }
    return hash;
  }

  private static byte[] serialize(HashMap<String, ByteIterator> hash)
    throws IOException {

    List<String> list = new ArrayList<String>();
    String obj = "";

    for (Map.Entry<String, ByteIterator> entry : hash.entrySet()) {
        String key = entry.getKey();
        String val = entry.getValue().toString();
        String element = key + "CAFEBABE" + val + "DEADBEEF";
        DEBUG("SERIALIZE. ELEMENT: %s\n", element);
        obj += element;
    }

    DEBUG ("SERIALIZED ROW: %s\n", obj);
    return bytes(obj);
  }

  private static HashMap<String, ByteIterator> deserialize(byte[] bytes)
        throws IOException, ClassNotFoundException {

    HashMap<String, ByteIterator> hash = new HashMap();
    String obj = new String (bytes);
    String[] elements = obj.split("DEADBEEF");

    for (String element: elements) {
        String[] f = element.split("CAFEBABE");
        hash.put(f[0], new ByteArrayByteIterator(bytes(f[1])));
    }

    if (dbg) {
        for (Map.Entry<String, ByteIterator> entry : hash.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue().toString();
            DEBUG("DESERIALIZE. KEY: %s, VAL: %s\n", key, val);
        }
    }
    return hash;
  }

  public void init() throws DBException {
    Properties props = getProperties();
    int fieldCount =
	Integer.parseInt(props.getProperty("fieldcount", "10"));

    fieldArrayIdx = new int[fieldCount];
    for (int i = 0; i < fieldCount; i++)
	    fieldArrayIdx[i] = i;

    fyShuffle(fieldArrayIdx, fieldCount + 1);

    String path = props.getProperty("leveldb.path");
    if (path == null)
      throw new DBException("path to LevelDB database required");

    format = props.getProperty("format");
    if (format == null)
      throw new DBException("Storage format is required <rowmajor/colmajor>");

    if (props.getProperty("dump") != null)
        dumpIt = Boolean.parseBoolean(props.getProperty("dump"));
    if (props.getProperty("debug") != null)
        dbg = Boolean.parseBoolean(props.getProperty("debug"));
    if (props.getProperty("allrows") != null)
        allRows = Boolean.parseBoolean(props.getProperty("allrows"));
    if (props.getProperty("numcols") != null)
        numCols = Integer.parseInt(props.getProperty("numcols"));

    Options options = new Options();
    options.createIfMissing(true);

    DEBUG ("Dump        : %b\n", dumpIt);
    DEBUG ("Debug       : %b\n", dbg);
    DEBUG ("AllRows     : %b\n", allRows);
    DEBUG ("NumCols     : %d\n", numCols);
    DEBUG ("Format      : %s\n", format);
    DEBUG ("Feld Count  : %d\n", fieldCount);

    try {
      db = factory.open(new File(path), options);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  private void dumpTable(boolean force) {
    if (dumpIt || force) {
        DBIterator iterator = db.iterator();
        iterator.seekToFirst();
        while (iterator.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iterator.next();
            String dbkey = new String (entry.getKey());
            System.out.println(dbkey);
        }
    }
  }

  public void cleanup() throws DBException {
    try {
      dumpTable(false);
      db.close();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  private DBIterator readFull (String dbSeekKey,
        HashMap<String, ByteIterator> result,
        DBIterator iterator) throws java.io.IOException {

    /* Assumes that the iterator is has alread seek-ed 
     * to the correct position
     */
    // long start = System.nanoTime();
    while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        String dbkey = new String (entry.getKey());
        String[] dbkeysplit = dbkey.split(DELIMITER);
	    DEBUG("READ KEY: %s, splitlen: %d\n",
		    dbkey, dbkeysplit.length);
        if (dbkeysplit.length != 3) break;

        String field = dbkeysplit[2];
	    DEBUG("dbkey: %s, dbSeekKey: %s\n", dbkey, dbSeekKey);
	    DEBUG("READ dbkey: %s, Field: %s, Value: %s\n",
	        dbkey, field, entry.getValue().toString());
        if (field.equals(TOMBSTONE)) break;
        result.put(field,
            new ByteArrayByteIterator(entry.getValue()));
    }
/*
    long duration = System.nanoTime() - start;
    System.out.println("readFull: dbSeekKey: " + dbSeekKey
	+ " Duration: " + duration/1e6);
*/
    return iterator;
  }

  private DBIterator _readRm(String rowStartKey, Set<String> fields,
    HashMap<String, ByteIterator> result, DBIterator dbIter)
    throws IOException {

	if (allRows) {
	    /* Column scans */
	    String[] fieldArray = fields.toArray(new String[0]);
	    dbIter.seekToFirst();
	    while (dbIter.hasNext()) {
           	Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
           	String dbkey = new String (entry.getKey());
           	String[] dbkeysplit = dbkey.split(DELIMITER);
		    /* 
		     * Caclulate the tombStone so that you can jump to the
		     * next row after reading reqd columns for the current row
		     */
           	String tombStone = concat(dbkeysplit[0], dbkeysplit[1]) + TOMBSTONE;
	        for (String column: fieldArray) {
                String lkey = concat(dbkeysplit[0], dbkeysplit[1]) + column;
                byte value[] = db.get(bytes(lkey));
		        /* !!! BEWARE !!! This DEBUG will take for ever to log */
		        // DEBUG("READ: %s, Value: %s\n", lkey, value);
                result.put(column, new ByteArrayByteIterator(value));
	        }
		    /* Jump to current row end */
		    dbIter.seek(bytes(tombStone));
		    /* If table end is not reached, go to next row */
		    if (dbIter.hasNext()) dbIter.next();
	    }
	} else {
	    dbIter.seek(bytes(rowStartKey));
	    if (fields == null) {
	        /* Retrieve all columns for a row */
            dbIter = readFull (rowStartKey, result, dbIter);
	    } else {
		    /* Read individual columns */
		    Iterator<String> it = fields.iterator();
		    while (it.hasNext()) {
                String column = it.next();
                String lkey = rowStartKey + column;
                byte value[] = db.get(bytes(lkey));
		        DEBUG("READ: %s, Value: %s\n", lkey, value);
                result.put(column, new ByteArrayByteIterator(value));
		    }
	    }
	}
	return dbIter;
  }

  public int readRm(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) throws IOException {
	    String rowStartKey = concat(table, key);
        DBIterator dbIter = db.iterator();
        _readRm(rowStartKey, fields, result, dbIter);
	    dbIter.close();
        return 0;
  }

  public int readCm(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) throws IOException {

    DBIterator dbIter = db.iterator();
	if (allRows) {
	    /* Column scans */
	    /* Column scan: Retrieve all rows for a column */
	    String[] fieldArray = fields.toArray(new String[0]);
	    Arrays.sort(fieldArray);
	    for (String column: fieldArray) {
		    String colStartKey = concat(table, column);
		    dbIter.seek(bytes(colStartKey));
            readFull (colStartKey, result, dbIter);
		    DEBUG("READ CM: result size: %d\n", result.size());
	    }
	} else {
	    String[] fieldArray = fields.toArray(new String[0]);
        /* Sort the columns so that we read sequentially */
	    Arrays.sort(fieldArray);
        DEBUG ("Read row. Row ID: %s\n", key);
		/* Read individual columns */
        for (String column: fieldArray) {
            String dbKey = concat (table, column) + key;
            byte value[] = db.get(bytes(dbKey));
		    DEBUG("READ: %s, Value: %s\n", dbKey, value);
            result.put(column, new ByteArrayByteIterator(value));
        }
        DEBUG ("Read row. NUM fields: %d\n", result.size());
	}
	dbIter.close();
	return 0;
  }

  public int readFr(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result)
        throws IOException, ClassNotFoundException {

    if (allRows) {
        DBIterator dbIter = db.iterator();
        dbIter.seekToFirst();
        while (dbIter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
            HashMap<String, ByteIterator> res = new HashMap();
            res = deserialize(entry.getValue());
            HashMap<String, ByteIterator> res_trimmed = new HashMap();
            if (fields != null) {
                Iterator it = fields.iterator();
                while (it.hasNext()) {
                    String field = (String) it.next();
                    for (Map.Entry<String, ByteIterator> e : res.entrySet()) {
                        if ((e.getKey()).equals(field)) {
                            res_trimmed.put(field, e.getValue());
                            break;
                        }
                    }
                }
                res = res_trimmed;
            }
            result.put(new String (entry.getKey()),
                new ByteArrayByteIterator(serialize(res)));
            dbIter.next();
        }
        dbIter.close();
    } else {
        byte value[] = db.get(bytes(concat(table, key)));
        HashMap<String, ByteIterator> res = new HashMap();
        res = deserialize(value);
        if (fields != null) {
            Iterator it = fields.iterator();
            while (it.hasNext()) {
                String field = (String) it.next();
                result.put(field, res.get(field));
            }
        }
    }
    return 0;
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {

/*
    // Shortcut
	if (format.equals("colmajor") && numCols == 0)
        numCols = fieldArrayIdx.length;
*/
    DEBUG ("READ: Num cols :%d: %s\n", numCols, format);
    if (numCols != 0) {
        /*
         * usually users specify the columns instead of numcols.
         * Here for benchmarking, I am using numcols to randomize
         * the column selection.
         */
	    fields = new HashSet<String>();
	    for (int i = 0; i < numCols; i++) {
	        fields.add("field" + Integer.toString(fieldArrayIdx[i]));
	    }
    } else if (format.equals("colmajor")) {
        /*
         * Populate the columns as user does not know what
         * columns exists
         */
		DEBUG ("READ: Populating fields\n");
        DBIterator dbIter = db.iterator();
        dbIter.seekToFirst();
	    fields = new HashSet<String>();
        while (dbIter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
            String lkey = new String (entry.getKey());
            String[] dbkeysplit = lkey.split(DELIMITER);
            DEBUG("Add field: %s\n", dbkeysplit[1]);
            fields.add(dbkeysplit[1]);
           	String tombStone = concat(dbkeysplit[0], dbkeysplit[1]) + TOMBSTONE;
            dbIter.seek(bytes(tombStone));
            dbIter.next();
        }
        try {
            dbIter.close();
        } catch (IOException e) {
            System.err.println("db.read: " + e.getMessage());
            return -1;
        }
		Iterator<String> it = fields.iterator();
        while (it.hasNext())
		    DEBUG ("READ: Field: %s\n", it.next());
    }

    try {
	if (format.equals("rowmajor"))
        return readRm(table, key, fields, result);
	else if (format.equals("colmajor"))
        return readCm(table, key, fields, result);
    else /* fullrow */
        return readFr(table, key, fields, result);
    } catch (ClassNotFoundException e) {
        System.err.println("db.read: " + e.getMessage());
        return -1;
    } catch (IOException e) {
        System.err.println("db.read: " + e.getMessage());
        return -1;
    }
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {

    DEBUG("Insert : format %s\n", format);
    try {
        if (format.equals("fullrow")) {
            DEBUG("Insert %s\n", concat(table, key));
            db.put(bytes(concat(table, key)), serialize(values));
            return 0;
        }
    } catch (IOException e) {
        System.err.println("db.put insert: " + e.getMessage());
        return -1;
    }

    try {
        WriteBatch batch = db.createWriteBatch();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            String col = entry.getKey();
            String llkey;
            String tombStone = null;
            if (format.equals("rowmajor")) {
                llkey = concat(table, key) + col;
    	        if (! tombStoneKeys.contains(key)) {
	                tombStoneKeys.add(col);
                    tombStone = concat(table, key) + TOMBSTONE;
	            }
            } else {
                llkey = concat(table, col) + key;
	            if (! tombStoneKeys.contains(col)) {
	                tombStoneKeys.add(col);
                    tombStone = concat(table, col) + TOMBSTONE;
    	        }
            }

            batch.put(bytes(llkey), entry.getValue().toArray());
	        if (tombStone != null)
                batch.put(bytes(tombStone), bytes("."));
        }
        db.write(batch);
        batch.close();
      } catch (org.iq80.leveldb.DBException e) {
            System.err.println("db.put insert: " + e.getMessage());
            return -1;
      } catch (IOException e) {
            System.err.println("db.put insert: " + e.getMessage());
            return -1;
      }
    return 0;
  }

  private int _deleteRm_(String table, String key) {
    try {
        DBIterator dbIter = db.iterator();
        String dbSeekKey = concat(table, key);
        dbIter.seek(bytes(dbSeekKey));
        while (dbIter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
            key = new String (entry.getKey());
            String[] dbkeysplit = key.split(DELIMITER);
            String dbKey = concat(dbkeysplit[0], dbkeysplit[1]);
            if (dbSeekKey.equals(dbKey)) {
                DEBUG ("DELETE: %s\n", key);
                db.delete(entry.getKey());
            } else {
                break;
            }
            dbIter.next();
        }
    } catch (Exception e) {
        System.err.println("db.delete: " + e.getMessage());
        return -1;
    }
    return 0;
  }

  private int deleteRm(String table, String key) {
    String[] fieldArray = new String[fieldArrayIdx.length];
    for (int i = 0; i < fieldArray.length; i++)
        fieldArray[i] = "field" + Integer.toString(i);

    try {
        DBIterator dbIter = db.iterator();
        for (String column: fieldArray) {
            String delKey = concat(table, key) + column;
            DEBUG("DELETE key: %s\n", delKey);
            db.delete(bytes(delKey));
        }
        dbIter.close();
    } catch (Exception e) {
        System.err.println("db.delet-RM: " + e.getMessage());
        return -1;
    }
    return 0;
  }
 
  private int deleteCm(String table, String key) {
    String[] fieldArray = new String[fieldArrayIdx.length];
    for (int i = 0; i < fieldArray.length; i++)
        fieldArray[i] = "field" + Integer.toString(i);

    try {
        DBIterator dbIter = db.iterator();
        for (String column: fieldArray) {
            String delKey = concat(table, column) + key;
            DEBUG("DELETE key: %s\n", delKey);
            db.delete(bytes(delKey));
        }
        dbIter.close();
    } catch (Exception e) {
        System.err.println("db.delete-CM: " + e.getMessage());
        return -1;
    }
    return 0;
  }

  @Override
  public int delete(String table, String key) {
      if (format.equals("rowmajor"))
            return deleteRm(table, key);
      else if (format.equals("colmajor"))
            return deleteCm(table, key);
      else /* fullrow */
            db.delete(bytes(concat(table, key)));
      return 0;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {

    try {
        if (format.equals("fullrow")) {
            WriteBatch batch = db.createWriteBatch();
            batch.put(bytes(concat(table, key)), serialize(values));
            db.write(batch);
            batch.close();
            return 0;
        }
    } catch (IOException e) {
        System.err.println("db.put insert: " + e.getMessage());
        return -1;
    }

    try {
        WriteBatch batch = db.createWriteBatch();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String llkey;
            if (format.equals("rowmajor"))
                llkey = concat(table, key) + entry.getKey();
            else
                llkey = concat(table, entry.getKey()) + key;
            DEBUG ("UPDATE: %s, Value: %s\n",
                llkey, new String (entry.getValue().toArray()));
            batch.put(bytes(llkey), entry.getValue().toArray());
        }
        db.write(batch);
        batch.close();
    } catch (java.io.IOException e) {
        System.err.println("db.update: " + e.getMessage());
        return -1;
    }
    return 0;
  }

  private int _scan(String table, String startKey, int recordCount,
    Set<String> fields, Vector<HashMap<String, ByteIterator>> result)
    throws IOException {

    int recordCountIn = recordCount;
    DBIterator dbIter = db.iterator();
    String dbSeekKey = concat(table, startKey);
    boolean allRowsLocal = allRows;
	String[] fieldArray = null;

    if (format.equals("colmajor")) {
	    fieldArray = fields.toArray(new String[0]);
        dbIter.seek(bytes(concat(table, fieldArray[0]) + startKey));
    } else {
        dbIter.seek(bytes(dbSeekKey));
    }

    String key = startKey;

    if (allRows)
        DEBUG ("SCAN allRows specified. Disabled.\n");

    allRows = false;
    while (dbIter.hasNext() && recordCount > 0) {
        HashMap<String, ByteIterator> res = new HashMap();
        if (format.equals("rowmajor")) {
            DEBUG ("SCAN START KEY: %s\n", dbSeekKey);
            dbIter = _readRm(dbSeekKey, fields, res, dbIter);
        } else {
            DEBUG ("SCAN START KEY: %s\n", concat(table, key));
            readCm(table, key, fields, res);
        }
        DEBUG ("SCAN: res.size: %d\n", res.size());
        if (! result.add(res)) {
            System.err.println("Could not add result for: " + dbSeekKey);
        }
        if (dbIter.hasNext()) {
            /* READ stopped because read saw a new row */
            Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
            String[] dbkeysplit = (new String (entry.getKey())).split(DELIMITER);
            if (format.equals("rowmajor")) {
                dbSeekKey = concat (dbkeysplit[0], dbkeysplit[1]);
            } else {
                /*
                 * Point to the current key so that we can get the
                 * row ID of the next row later in the next round.
                 */
                key = dbkeysplit[2];
                dbIter.next();
            }
        } else {
            /* READ stopped because read hit the end of table */
            break;
        }
        recordCount--;
    }
    if (recordCount > 0)
        DEBUG ("Unsatisfied Record Count: %d, Total: %d, Start Key: %s\n",
            recordCount, recordCountIn, startKey);

    DEBUG ("SCAN: result vector.size: %d\n", result.size());
    dbIter.close();
    allRows = allRowsLocal;
    if (allRows)
        DEBUG ("SCAN allRows Re-Enabled.\n");
    return 0;
  }

  private int _scanFr(String table, String startKey,
        int recordCount, Set<String> fields,
        Vector<HashMap<String, ByteIterator>> result) 
    throws IOException, ClassNotFoundException {

    DBIterator dbIter = db.iterator();
    dbIter.seek(bytes(concat(table, startKey)));
    while (dbIter.hasNext() && recordCount > 0) {
        Map.Entry<byte[], byte[]> entry = dbIter.next();
        HashMap<String, ByteIterator> res =
            deserialize(entry.getValue());
        DEBUG ("SCAN: res.size: %d\n", res.size());
        if (! result.add(res)) {
            System.err.println("Could not add result for: " + entry.getKey());
        }
        recordCount --;
    }
    dbIter.close();

    return 0;
  }
  @Override
  public int scan(String table, String startKey, int recordCount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    DEBUG("SCAN recordCount: %d, NumCols: %d, format: %s\n",
        recordCount, numCols, format);

/*
    // Shortcut
	if (format.equals("colmajor") && numCols == 0)
        numCols = fieldArrayIdx.length;
*/

    if (numCols != 0) {
	    fields = new HashSet<String>();
	    for (int i = 0; i < numCols; i++) {
	        fields.add("field" + Integer.toString(fieldArrayIdx[i]));
	    }
    } else if (format.equals("colmajor")) {
        /*
         * Populate the columns as user does not know what
         * columns exists
         */
		DEBUG ("SCAN: Populating fields\n");
        DBIterator dbIter = db.iterator();
        dbIter.seekToFirst();
	    fields = new HashSet<String>();
        while (dbIter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = dbIter.peekNext();
            String key = new String (entry.getKey());
            String[] dbkeysplit = key.split(DELIMITER);
            DEBUG("Add field: %s\n", dbkeysplit[1]);
            fields.add(dbkeysplit[1]);
           	String tombStone = concat(dbkeysplit[0], dbkeysplit[1]) + TOMBSTONE;
            dbIter.seek(bytes(tombStone));
            dbIter.next();
        }
        try {
            dbIter.close();
        } catch (IOException e) {
            System.err.println("db.scan: " + e.getMessage());
            return -1;
        }

		Iterator<String> it = fields.iterator();
        while (it.hasNext())
		    DEBUG ("SCAN: Field: %s\n", it.next());
    }


    try {
        if (format.equals("fullrow")) {
            return _scanFr(table, startKey, recordCount, fields, result);
        } else {
            return _scan(table, startKey, recordCount, fields, result);
        }
    } catch (java.io.IOException e) {
        System.err.println("db.scan: " + e.getMessage());
        return -1;
    } catch (ClassNotFoundException e) {
        System.err.println("db.scan: " + e.getMessage());
        return -1;
    }
  }
}
