package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;



import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	static Object wait1 = new Object();
	Button put1;
	Button put2;
	Button put3;
	static TextView tv;
	Button get;
	Button ldump;
	static boolean failed =false;
	public static Cursor mycursor;
	
	Context myContext;
	String dispstr="";

	String keystr="";
	String valstr="";

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());


		put1 = (Button) findViewById(R.id.button1);
		put2 = (Button) findViewById(R.id.button2);
		put3 = (Button) findViewById(R.id.button3);
		ldump =(Button) findViewById(R.id.button4);
		get =(Button) findViewById(R.id.button5);

		put1.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				// TODO Auto-generated method stub
				new Insert1().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				Log.d("put1","insert");

			}
		});
		put2.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				// TODO Auto-generated method stub
				new Insert2().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				Log.d("put1","insert");

			}
		});
		put3.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				// TODO Auto-generated method stub
				new Insert3().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				Log.d("put1","insert");

			}
		});	
		get.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				// TODO Auto-generated method stub



				new get().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				Log.d("get","insert");

			}
		});
		ldump.setOnClickListener(new View.OnClickListener() {
			public void onClick(View v) {
				// TODO Auto-generated method stub



				new ldump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
				Log.d("get","insert");

			}
		});

	}
	class Insert1 extends AsyncTask<String, String, Void> {

		
		@Override
		protected Void doInBackground(String... str) {

			int seqno=0;
			String putval ="put1";
			String clientport="";

			int failednode = 0;
			String x = SimpleDynamoProvider.num;
			final String vote = "PING"+" "+x;

			

			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.succ)*2),vote);

			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.pred )*2),vote);
			
			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(!SimpleDynamoProvider.predflag)
			{
				failed= true;
				 failednode=(SimpleDynamoProvider.pred );
				 Log.d("predflag in put1",Integer.toString(failednode));
				
			}
			if(!SimpleDynamoProvider.sucflag){
				failed = true;
				 failednode =(SimpleDynamoProvider.succ );
				 Log.d("succflag in put1",Integer.toString(failednode));
			}
			for (int j = 0; j < 20; j++) {
				/*ContentValues key_val = new ContentValues();
				key_val.put(MySqliteHelper.PROVIDER_KEY, Integer.toString(seqno));
				key_val.put(MySqliteHelper.PROVIDER_VALUE, putval+seqno);
				Uri uri = getContentResolver().insert(SimpleDynamoProvider.CONTENT_URI ,
						key_val);*/
				String iterhash;
				try {

					iterhash = SimpleDynamoProvider.genHash(Integer.toString(j));

					String four = SimpleDynamoProvider.genHash("5554");
					String six = SimpleDynamoProvider.genHash("5556");
					String eight = SimpleDynamoProvider.genHash("5558");
					if(iterhash.compareTo(six)>0 &&iterhash.compareTo(four)<0) {
						clientport="11108";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);
					}
					else if (iterhash.compareTo(four)>0 &&iterhash.compareTo(eight)<0){
						clientport="11116";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					else if(iterhash.compareTo(eight)>0 ||iterhash.compareTo(six)<0){
						clientport="11112";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (NoSuchAlgorithmException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				seqno++;
			}
			seqno=0;
			return null;
		}
		protected void onProgressUpdate(String...strings) {

			tv.append(strings[0]);

			return;
		}
	}
	class ldump extends AsyncTask<String, String, Void> {

		@Override
		protected Void doInBackground(String... str) {
			int seqno = 0;


			for (int j = 0; j < 20; j++) {
				String[] projection = {MySqliteHelper.PROVIDER_VALUE };
				String selection = MySqliteHelper.PROVIDER_KEY+"=" + "'" +Integer.toString(j)+"'";


				mycursor = getContentResolver().query(
						SimpleDynamoProvider.CONTENT_URI , projection, selection,
						null, null);

				if (mycursor != null && mycursor.getCount() != 0) {
					while (mycursor.moveToNext()) {
						keystr =Integer.toString(j);
						valstr= mycursor.getString(mycursor.getColumnIndexOrThrow(MySqliteHelper.PROVIDER_VALUE));
						dispstr = "\""+ "<"+ keystr + ","+valstr+ ">"+ "\""+"\n";
					}
				}

				publishProgress(dispstr);

				seqno++;
			}
			seqno=0;
			return null;
		}
		protected void onProgressUpdate(String...strings) {

			tv.append(strings[0]);

			return;
		}

	}


	class Insert2 extends AsyncTask<String, String, Void> {


		@Override
		protected Void doInBackground(String... str) {
			int seqno=0;
			String putval ="put2";
			String clientport="";

			int failednode = 0;
			String x = SimpleDynamoProvider.num;
			final String vote = "PING"+" "+x;

			

			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.succ)*2),vote);

			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.pred )*2),vote);
			
			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(!SimpleDynamoProvider.predflag)
			{
				failed= true;
				 failednode=(SimpleDynamoProvider.pred );
				 Log.d("predflag in put1",Integer.toString(failednode));
				
			}
			if(!SimpleDynamoProvider.sucflag){
				failed = true;
				 failednode =(SimpleDynamoProvider.succ );
				 Log.d("succflag in put1",Integer.toString(failednode));
			}
			for (int j = 0; j < 20; j++) {
				/*ContentValues key_val = new ContentValues();
				key_val.put(MySqliteHelper.PROVIDER_KEY, Integer.toString(seqno));
				key_val.put(MySqliteHelper.PROVIDER_VALUE, putval+seqno);
				Uri uri = getContentResolver().insert(SimpleDynamoProvider.CONTENT_URI ,
						key_val);*/
				String iterhash;
				try {

					iterhash = SimpleDynamoProvider.genHash(Integer.toString(j));

					String four = SimpleDynamoProvider.genHash("5554");
					String six = SimpleDynamoProvider.genHash("5556");
					String eight = SimpleDynamoProvider.genHash("5558");
					if(iterhash.compareTo(six)>0 &&iterhash.compareTo(four)<0) {
						clientport="11108";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);
					}
					else if (iterhash.compareTo(four)>0 &&iterhash.compareTo(eight)<0){
						clientport="11116";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					else if(iterhash.compareTo(eight)>0 ||iterhash.compareTo(six)<0){
						clientport="11112";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (NoSuchAlgorithmException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				seqno++;
			}
			seqno=0;
			return null;
		}
		protected void onProgressUpdate(String...strings) {

			tv.append(strings[0]);

			return;
		}
	}

	class Insert3 extends AsyncTask<String, String, Void> {

		@Override
		protected Void doInBackground(String... str) {
			int seqno=0;
			String putval ="put3";
			String clientport="";

			int failednode = 0;
			String x = SimpleDynamoProvider.num;
			final String vote = "PING"+" "+x;

			

			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.succ)*2),vote);

			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.pred )*2),vote);
			
			synchronized(wait1) {
				try {
					wait1.wait(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(!SimpleDynamoProvider.predflag)
			{
				failed= true;
				 failednode=(SimpleDynamoProvider.pred );
				 Log.d("predflag in put1",Integer.toString(failednode));
				
			}
			if(!SimpleDynamoProvider.sucflag){
				failed = true;
				 failednode =(SimpleDynamoProvider.succ );
				 Log.d("succflag in put1",Integer.toString(failednode));
			}
			for (int j = 0; j < 20; j++) {
				/*ContentValues key_val = new ContentValues();
				key_val.put(MySqliteHelper.PROVIDER_KEY, Integer.toString(seqno));
				key_val.put(MySqliteHelper.PROVIDER_VALUE, putval+seqno);
				Uri uri = getContentResolver().insert(SimpleDynamoProvider.CONTENT_URI ,
						key_val);*/
				String iterhash;
				try {

					iterhash = SimpleDynamoProvider.genHash(Integer.toString(j));

					String four = SimpleDynamoProvider.genHash("5554");
					String six = SimpleDynamoProvider.genHash("5556");
					String eight = SimpleDynamoProvider.genHash("5558");
					if(iterhash.compareTo(six)>0 &&iterhash.compareTo(four)<0) {
						clientport="11108";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);
					}
					else if (iterhash.compareTo(four)>0 &&iterhash.compareTo(eight)<0){
						clientport="11116";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						 publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					else if(iterhash.compareTo(eight)>0 ||iterhash.compareTo(six)<0){
						clientport="11112";
						if(clientport.equals(Integer.toString(failednode*2))){
							if(Integer.toString(failednode).equals("5554")){
								clientport ="11116";
							
							}
							else if(Integer.toString(failednode).equals("5556")){
								clientport ="11108";
								

							}
							else if(Integer.toString(failednode).equals("5558")){
								clientport ="11112";
						

							}
						}
						if(failed){
						publishProgress("failed node is "+failednode+" and forwarded to "+ clientport+"\n");
						}String insertmessage= "INSERT"+" "+Integer.toString(seqno)+" "+ putval+seqno+" "+"NEW"+ " "+clientport ;
						new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,clientport,insertmessage);

					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (NoSuchAlgorithmException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				seqno++;
			}
			seqno=0;
			return null;		}
		protected void onProgressUpdate(String...strings) {

			tv.append(strings[0]);

			return;
		}
	}
	class get extends AsyncTask<Context, String, Void> {

		@Override
		protected Void doInBackground(Context... arg0) {
			for (int j = 0; j < 20; j++) {
				
			
				final String get = "GET"+" "+ Integer.toString(j)+" "+SimpleDynamoProvider.num;

				Object wait = new Object();

				new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.succ)*2),get);

				synchronized(wait) {
					try {
						wait.wait(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				boolean failed =false;
				new SimpleDynamoProvider.ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,Integer.toString((SimpleDynamoProvider.pred )*2),get);
				
				synchronized(wait) {
					try {
						wait.wait(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				String[] projection = {MySqliteHelper.PROVIDER_VALUE };
				String selection = MySqliteHelper.PROVIDER_KEY+"=" + "'" +Integer.toString(j)+"'";

		

			
			}
			return null;
		}
		protected void onProgressUpdate(String...strings) {
//			tv.append(strings[0]);

			return;
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

}


