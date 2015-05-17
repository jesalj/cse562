package edu.buffalo.cse562.Load;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class LoadPhase {

	Database myDatabase = null;
	File path = null;
	Environment dbEnv = null;
	private HashMap<String, SecondaryDatabase> secDbMap = new HashMap<>();
	public LoadPhase(Database myDatabase, File path, Environment env) {
		this.myDatabase = myDatabase;
		this.path = path;
		dbEnv = env;
	}
	
	public HashMap<String, SecondaryDatabase> getSecDbMap(){
		return secDbMap;
	}
	public Database processLineItem() {
		// Only difference for the primary key, using seq num
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		SecondaryDatabase mySecDb = null;
		Integer seqNum = 0;
		// SecondaryConfig mySecConfig = new SecondaryConfig();

		/* Secondary Index */
		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {

				key = seqNum.toString();

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myBinding.objectToEntry(line, theData);

				myDatabase.put(null, theKey, theData);
				seqNum++;
			}

			/*
			 * mySecConfig.setAllowCreate(true);
			 * mySecConfig.setSortedDuplicates(true); // Open the secondary. //
			 * Key creators are described in the next section.
			 * SecIndexKeyCreator keyCreator = new
			 * SecIndexKeyCreator(myBinding); keyCreator.setIndex(4);
			 * 
			 * // Get a secondary object and set the key creator on it.
			 * mySecConfig.setKeyCreator(keyCreator);
			 * mySecConfig.setAllowPopulate(true);
			 * 
			 * // Perform the actual open String secDbName =
			 * "mySecondaryDatabase"; mySecDb =
			 * dbEnv.openSecondaryDatabase(null, secDbName, myDatabase,
			 * mySecConfig);
			 */
			mySecDb = createSecDb(myBinding, myDatabase, 11,
					"LINEITEM.SHIPMODE");
			//getRecord(mySecDb, "8");
			secDbMap.put("LINEITEM.SHIPMODE", mySecDb);

		} catch (IOException e) {
			e.printStackTrace();

		}
		mySecDb.close();
		// getAllRecord();
		return myDatabase;
	}

	public Database processOrders() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		SecondaryDatabase mySecDb = null;

		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);
		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myBinding.objectToEntry(line, theData);
				myDatabase.put(null, theKey, theData);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		mySecDb = createSecDb(myBinding, myDatabase, 5, "ORDERS.ORDERDATE");
		//getRecord(mySecDb, "8");
		secDbMap.put("ORDERS.ORDERDATE", mySecDb);
		mySecDb.close();
		
		// getAllRecord();
		return myDatabase;
	}

	public Database processCustomer() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);
		SecondaryDatabase mySecDb = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myBinding.objectToEntry(line, theData);
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		mySecDb = createSecDb(myBinding, myDatabase, 7, "CUSTOMER.MKTSEGMENT");
		//getRecord(mySecDb, "8");
		secDbMap.put("CUSTOMER.MKTSEGMENT", mySecDb);
		mySecDb.close();
		// getAllRecord();
		return myDatabase;
	}

	public Database processSupplier() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processPartSupp() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processNation() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processRegion() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				theData = new DatabaseEntry(line.getBytes("UTF-8"));
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public void getAllRecord() {
		Cursor cursor = null;
		try {

			cursor = myDatabase.openCursor(null, null);

			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				String keyString = new String(foundKey.getData(), "UTF-8");
				String dataString = new String(foundData.getData(), "UTF-8");

			}
		} catch (DatabaseException | UnsupportedEncodingException de) {
			System.err.println("Error accessing database." + de);
		} finally {
			// Cursors must be closed.
			cursor.close();
		}
	}

	public void getRecord(SecondaryDatabase mySecDb, String aKey) {

		Cursor cursor = null;
		try {

			// Database and environment open omitted for brevity

			// Open the cursor.
			cursor = mySecDb.openCursor(null, null);

			// Cursors need a pair of DatabaseEntry objects to operate. These
			// hold
			// the key and data found at any given position in the database.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			// To iterate, just call getNext() until the last database record
			// has
			// been read. All cursor operations return an OperationStatus, so
			// just
			// read until we no longer see OperationStatus.SUCCESS
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				// getData() on the DatabaseEntry objects returns the byte array
				// held by that object. We use this to get a String value. If
				// the
				// DatabaseEntry held a byte array representation of some other
				// data type (such as a complex object) then this operation
				// would
				// look considerably different.
				String keyString = new String(foundKey.getData(), "UTF-8");
				String dataString = new String(foundData.getData(), "UTF-8");
				System.out.println("Key | Data : " + keyString + " | "
						+ dataString + "");
			}
		} catch (DatabaseException | UnsupportedEncodingException de) {
			System.err.println("Error accessing database." + de);
		} finally {
			// Cursors must be closed.
			cursor.close();
		}
	}

	public SecondaryDatabase createSecDb(EntryBinding binding,
			Database myDatabase, int index, String name) {
		SecondaryConfig mySecConfig = new SecondaryConfig();
		mySecConfig.setAllowCreate(true);
		mySecConfig.setSortedDuplicates(true);
		// Open the secondary.
		// Key creators are described in the next section.
		SecIndexKeyCreator keyCreator = new SecIndexKeyCreator(binding);
		keyCreator.setIndex(index);

		// Get a secondary object and set the key creator on it.
		mySecConfig.setKeyCreator(keyCreator);
		mySecConfig.setAllowPopulate(true);

		// Perform the actual open
		String secDbName = name;
		SecondaryDatabase mySecDb = dbEnv.openSecondaryDatabase(null,
				secDbName, myDatabase, mySecConfig);
		return mySecDb;
	}
}
