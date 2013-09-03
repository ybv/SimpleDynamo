package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class MySqliteHelper extends SQLiteOpenHelper {
	
	private static final String DATABASE_NAME = "Message_Database";
	private static final int DATABASE_VERSION = 5;
	public static final String PROVIDER_KEY = "key";
	public static final String PROVIDER_VALUE = "value";
	public MySqliteHelper(Context context) 
	
	{super(context, DATABASE_NAME, null, DATABASE_VERSION);
	//System.out.println(context.getDatabasePath(MySqliteHelper.DATABASE_NAME).toString)
	}
	


	@Override
	public void onUpgrade(SQLiteDatabase database, int oldVersion, int newVersion) {
		// TODO Auto-generated method stub
		/*Log.d( "Upgrading database from version "
				+ oldVersion + " to " + newVersion
				+ ", which will destroy all old data");*/
				database.execSQL("DROP TABLE IF EXISTS " + myTable.DB_TABLE);
				onCreate(database);
	}


	@Override
	public void onCreate(SQLiteDatabase database) {
		// TODO Auto-generated method stub
		myTable.onCreate(database);
		
	}
}
