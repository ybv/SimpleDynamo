package edu.buffalo.cse.cse486586.simpledynamo;


	import android.database.sqlite.SQLiteDatabase;
	import android.util.Log;

	/*reference- http://www.vogella.de/articles/AndroidSQLite/article.html*/ 
	public class myTable {

		public static final String DB_TABLE = "GroupChatMessenger";
		public static final String PROVIDER_KEY = "key";
		public static final String PROVIDER_VALUE = "value";
		public static void onCreate(SQLiteDatabase database) 
			{
			database.execSQL("create table " + DB_TABLE	+ "(" + PROVIDER_KEY + " VARCHAR(15) PRIMARY KEY," + PROVIDER_VALUE	+ " VARCHAR(15));");
			Log.d("Creation of Database","On create called");
			}
		
		}



