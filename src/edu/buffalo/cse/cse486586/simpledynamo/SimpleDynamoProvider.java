package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;





import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


	public String clportno;
	public String clportno1;
	public static TelephonyManager tel; 
	public static int succ ;
	public static int pred ;
	public static int count =1;
	public static  boolean sucflag=false;
	public static boolean predflag=false;
	String temp;
	static  String num;
	boolean recflag =false;
	private static MySqliteHelper dab;
	public int version=0;
	final HashSet<String> h2 = new HashSet<String>();
	private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";

	public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY + "/");

	Map<String, String> vermap = new TreeMap();
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {//values - key and value 

		//values.
		long row = 0;
		recflag=true;
		SQLiteDatabase sqldbase = dab.getWritableDatabase();
		Log.d("Content Provider","Insert");
		Uri newuri = ContentUris.withAppendedId(uri, row);
		//sqldbase.delete(myTable.DB_TABLE, null, null);
		//Log.d("Content Provider","deleted previous");
		System.out.println(values);
		row = sqldbase.replace(myTable.DB_TABLE, null,values);
		getContext().getContentResolver().notifyChange(uri, null);
		return newuri;

	}


	/*private boolean node_exists(String key) throws NoSuchAlgorithmException {
		//String key1 = Integer.toString(key);
		String keyHash=genHash(key);
		String myhash=genHash(num);
		String predhash=genHash(pred);
		String succhash=genHash(succ);
		System.out.println(" in node_exists predecessor is "+pred+ "successor is"+ succ);
		if((predhash.compareTo(myhash)>=0)&&(keyHash.compareTo(pred)>0||keyHash.compareTo(myhash)<=0))
		{    
			return true;
		}
		else if(keyHash.compareTo(myhash)<=0 && keyHash.compareTo(predhash)>0 )
		{
			return true;
		}

		else 
		{  
			return false;
		}
	}
	 */
	@Override
	public boolean onCreate() {

		Log.d("creating ","content provider");
		dab = new MySqliteHelper(getContext());
		tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		num = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		Log.d("Number",num);


		try{	
			String req ="";
			ServerSocket serverSocket = null;
			serverSocket = new ServerSocket(10000);
			Log.d("server thread started", "server thread started");

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			System.out.println("server thread is called");
			if(num.equals("5554")){
				succ = 5558;
				pred = 5556;
			}
			else if(num.equals("5556")){
				succ =5554;
				pred =5558;

			}
			else if(num.equals("5558")){
				succ = 5556;
				pred = 5554;

			}
			String termessage = "IMBACK" +" "+ num;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(succ*2),termessage);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(pred*2),termessage);
		}catch(IOException ee) {
			ee.printStackTrace();
			Log.d("server thread started nooo", "server thread started nooo");
			System.out.println("server thread calling exception" );

		}
		return true;





	}


	class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket...sockets) {	// T.his is a thread whatever we are running is run in background

			System.out.println("servertask");
			ServerSocket serverSocket = sockets[0];


			Log.d("vaibahv4",serverSocket + "");
			Socket ssocket;
			try {
				while(true) {
					ssocket = serverSocket.accept();
					//System.out.println("server coming inside while");
					BufferedReader br = new BufferedReader(new InputStreamReader(ssocket.getInputStream()));
					String msg = br.readLine();
					publishProgress(msg);
					//	ssocket.close();

				}
			}catch(IOException e) {
				e.printStackTrace();
				System.out.println("servtask not done"); 
			} 
			return null;
		}

		protected void onProgressUpdate(String...msg)	{// Coordinate the message with the main activity

			String infromclient = msg[0];
			Log.d("infromclient----",msg[0]);
			String order ="";
			int sendercount =0;
			String savior=""; 
			//sucflag = false;
			//SimpleDynamoActivity.tv.setText("");
			//predflag =false;
			String[] splits = infromclient.split(" ");
			/*	for( String s : splits){
					SimpleDynamoActivity.tv.append(s);
				}*/
			String keystr ="";
			String valstr= "";
			String dispstr ="";
			if( splits[0].equals("IMBACK") && recflag){


				if(splits[1].equals(Integer.toString(pred))){

					for( int j =0; j<20 ; j++){
						String[] projection = {MySqliteHelper.PROVIDER_VALUE };
						String selection = MySqliteHelper.PROVIDER_KEY+"=" + "'" +Integer.toString(j)+"'";
						SimpleDynamoActivity.mycursor = query(
								SimpleDynamoProvider.CONTENT_URI , projection, selection,
								null, null);

						if (SimpleDynamoActivity.mycursor != null && SimpleDynamoActivity.mycursor.getCount() != 0) {
							while (SimpleDynamoActivity.mycursor.moveToNext()) {
								keystr =Integer.toString(j);
								valstr= SimpleDynamoActivity.mycursor.getString(SimpleDynamoActivity.mycursor.getColumnIndexOrThrow(MySqliteHelper.PROVIDER_VALUE));
								dispstr = "SUCCGET"+" "+ keystr +" "+valstr;
							}
						}

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(Integer.parseInt(splits[1])*2),dispstr);

					}
				}

			}
			if( splits[0].equals("SUCCGET")){

				String key= splits[1];
				String val= splits[2];
				ContentValues values = new ContentValues();
				//Log.d("inside insert after recovery ","insert called");


				//SimpleDynamoActivity.tv.append(key+" <-key value in new -> " +val+ "\n" );
				values.put(myTable.PROVIDER_KEY,key);
				values.put(myTable.PROVIDER_VALUE, val);
				Uri uri = insert(SimpleDynamoProvider.CONTENT_URI ,values);

			}



			if( splits[0].equals("PING")){

				//send ping ack
				//sendercount ++;

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(Integer.parseInt(splits[1])*2),"PINGACK"+" "+num);
				//	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,splits[1],"PINGACK"+" "+Integer.toString(pred));
				//System.out.println( sendercount+ "st coordinator sending ping ack to the sender");
			}
			 if( splits[0].equals("PINGACK")){

			
				if( splits[1].equals(Integer.toString(succ))){
					Log.d("splits -1 inside succ match",splits[1]);
					sucflag=true;
				}
				if( splits[1].equals(Integer.toString(pred))){
					Log.d("splits -1 inside pred match",splits[1]);
					predflag=true;
				}
				synchronized(SimpleDynamoActivity.wait1) {

					SimpleDynamoActivity.wait1.notify();
				}
				/*synchronized(SimpleDynamoActivity.wait) {
					SimpleDynamoActivity.wait.notify();
				}*/
			}
			if(splits[0].equals("INSERT") && splits[3].equals("NEW")){
				/*	"INSERT"+" "+key+" "+val;*/
				String key= splits[1];
				String val= splits[2];
				ContentValues values = new ContentValues();
			//	Log.d("inside insert","insert called");

				if(!vermap.containsKey(key)){
					vermap.put(key, Integer.toString(version));
				}else{
					version ++;
					vermap.put(key, Integer.toString(version));
				}




				//SimpleDynamoActivity.tv.append(key+" <-key value in new -> " +val+ "\n" );
				values.put(myTable.PROVIDER_KEY,key);
				values.put(myTable.PROVIDER_VALUE, val);
				Uri uri = insert(SimpleDynamoProvider.CONTENT_URI ,
						values);
				// REPLICATE 

				if(splits[4].equals("11108")){
					String forwardnext = "INSERT"+" "+splits[1]+" "+ splits[2]+" "+"REPLICATE"+ " "+"11108" ;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11112",forwardnext);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11116",forwardnext);
				}
				else if(splits[4].equals("11112")){
					String forwardnext = "INSERT"+" "+splits[1]+" "+ splits[2]+" "+"REPLICATE"+ " "+"11112" ;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11108",forwardnext);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11116",forwardnext);
				}
				else if(splits[4].equals("11116")){
					String forwardnext = "INSERT"+" "+splits[1]+" "+ splits[2]+" "+"REPLICATE"+ " "+"11116" ;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11108",forwardnext);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"11112",forwardnext);
				}

			}
			else if(splits[0].equals("INSERT") && splits[3].equals("REPLICATE")){


				String key= splits[1];
				String val= splits[2];
				ContentValues values = new ContentValues();
			//	Log.d("inside insert REPLICATING","insert called");

				
			
					//SimpleDynamoActivity.tv.append(key+" <-key valu in repe-> " +val+ "\n" );
				values.put(myTable.PROVIDER_KEY,key);
				values.put(myTable.PROVIDER_VALUE, val);
				Uri uri = insert(SimpleDynamoProvider.CONTENT_URI ,
						values);
			}
			if( splits[0].equals("GET")){

				//send ping ack
				//sendercount ++;

				//query here

				String[] projection = {MySqliteHelper.PROVIDER_VALUE };
				String selection = MySqliteHelper.PROVIDER_KEY+"=" + "'" +splits[1]+"'";
				SimpleDynamoActivity.mycursor = query(
						SimpleDynamoProvider.CONTENT_URI , projection, selection,
						null, null);

				if (SimpleDynamoActivity.mycursor != null && SimpleDynamoActivity.mycursor.getCount() != 0) {
					while (SimpleDynamoActivity.mycursor.moveToNext()) {
						keystr =splits[1];
						valstr= SimpleDynamoActivity.mycursor.getString(SimpleDynamoActivity.mycursor.getColumnIndexOrThrow(MySqliteHelper.PROVIDER_VALUE));
						dispstr = "SELFGETDISP"+" "+"\""+ "<"+ keystr + ","+valstr+ ">"+ "\""+"\n";
					}
				}

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,Integer.toString(Integer.parseInt(splits[2])*2),dispstr);
				//SimpleDynamoActivity.tv.append(dispstr);
				//System.out.println( sendercount+ "st coordinator sending ping ack to the sender");
			}
			if( splits[0].equals("SELFGETDISP")){
				
				
				String[] projection = {MySqliteHelper.PROVIDER_VALUE };
				String selection = MySqliteHelper.PROVIDER_KEY+"=" + "'" +splits[1]+"'";
				SimpleDynamoActivity.mycursor = query(
						SimpleDynamoProvider.CONTENT_URI , projection, selection,
						null, null);

				if (SimpleDynamoActivity.mycursor != null && SimpleDynamoActivity.mycursor.getCount() != 0) {
					while (SimpleDynamoActivity.mycursor.moveToNext()) {
						keystr =splits[1];
						valstr= SimpleDynamoActivity.mycursor.getString(SimpleDynamoActivity.mycursor.getColumnIndexOrThrow(MySqliteHelper.PROVIDER_VALUE));
						dispstr ="\""+ "<"+ keystr + ","+valstr+ ">"+ "\""+"\n";
					}
				}
				Log.d("dispstr is this -------",dispstr);
			
					  if (!h2.add(splits[1])) {
						  SimpleDynamoActivity.tv.append(splits[1]);
					  }
				
				// you might want to print data from its own 
			}

		}
	}
	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		@SuppressWarnings("resource")
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	public static class ClientTask extends AsyncTask<String,String,Void>
	{

		@Override
		protected Void doInBackground(String... number) {
			System.out.println(" entered backgroud on clienttask");
			try {	
				// h
				int quorum = 0;
				//System.out.println("from server"+ number[0]);
				Socket socket;
				if(number[1].equals("PINGACK")){
					quorum ++;
				}

			//	System.out.println(" quorum value "+ quorum );
			//	System.out.println("client progress");
				//tv.append(parms[0] +"\n");
				//String[] infromactivity= number[1].split(" ");
				//System.out.println(" the in from activity ");
				socket = new Socket("10.0.2.2", Integer.parseInt(number[0]));

			//	Log.d("client works at",number[0]);
			//	Log.d("vaibahv5",socket + "");

				Log.d("client message being sent to the server",number[1]);
				PrintWriter out = null;

				out = new PrintWriter(new DataOutputStream(socket.getOutputStream()));
				//System.out.println(number[0]);

				System.out.println(number[1]);
				out.print(number[1]);//send phpne number
			//	System.out.println("sending to server");
				//System.out.println(out);
				//	publishProgress(out);
				out.flush();

				socket.close();

			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("back client");

			}
			return null;
		}


	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		SQLiteQueryBuilder qB = new SQLiteQueryBuilder();
		qB.setTables(myTable.DB_TABLE);
		Log.d("selection in query",selection);
		Log.d("query"," Content Provider");
		SQLiteDatabase db = dab.getWritableDatabase();
		System.out.println(dab);
		Cursor cursur = qB.query(db, projection, selection, null, null,null,null,null,null);
		System.out.println(cursur);
		cursur.setNotificationUri(getContext().getContentResolver(), uri);
		return cursur;

	}

	@Override
	public int update(Uri arg0, ContentValues arg1, String arg2, String[] arg3) {
		// TODO Auto-generated method stub
		return 0;
	}

}
