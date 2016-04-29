package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private static final String KEYVALUE_TABLE_NAME = "dynamo";
    private static final int SERVER_PORT = 10000;
    private static final String MSG_REQUEST_TYPE = "MSG_REQUEST_TYPE";
    private static final String SENDER_PORT = "SENDER_PORT";
    private static final String INSERT = "INSERT";
    private static final String INSERT_REPLICA = "INSERT_REPLICA";
    private static final String SUCCESS = "SUCCESS";
    private static final String QUERY = "QUERY";
    private static final String QUERY_ALL = "QUERY_ALL";
    private static final String FORWARDING_PORT = "FORWARDING_PORT";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String STAR_SIGN = "*";
    private static final String AT_SIGN = "@";
    private static final String PROVIDER_URI = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
    private static LinkedList<String> ringFormation = new LinkedList<String>(Arrays.asList("5562","5556","5554","5558","5560"));

    private SQLiteDatabase database;

    private static String serverPort;


    private Executor myExec = Executors.newSingleThreadExecutor();

    public static class KeyValueOpenHelper extends SQLiteOpenHelper {

        private static final String DATABASE_NAME = "PA4";
        private static final int DATABASE_VERSION = 2;

        private static final String KEY = "key";
        private static final String VALUE = "value";
        private static final String KEYVALUE_TABLE_CREATE =
                "CREATE TABLE " + KEYVALUE_TABLE_NAME + " (" +
                        KEY + " TEXT PRIMARY KEY, " +
                        VALUE + " TEXT);";

        KeyValueOpenHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL("DROP TABLE IF EXISTS " + KEYVALUE_TABLE_NAME);
            db.execSQL(KEYVALUE_TABLE_CREATE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int a, int b) {
            //Do nothing
        }
    }

    private void sendMsgUsingSocket(String m, String port) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        out.write(m);
        out.close();
        socket.close();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        Log.d(TAG, "delete: uri: " + uri + " selection: " + selection);

        if (STAR_SIGN.equals(selection) || AT_SIGN.equals(selection))
            database.execSQL("DELETE FROM " + KEYVALUE_TABLE_NAME);
        else
            database.execSQL("DELETE FROM " + KEYVALUE_TABLE_NAME + " WHERE key=" + "'" + selection + "'");
        return 0;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.d(TAG, "insert: " + "ContentValues: " + values + ", " + "Uri: " + uri.toString());

		String key = values.getAsString("key");
		String value = values.getAsString("value");

		Log.d(TAG, "insert: Got Key: " + key + " value: " + value);

		try {
            if(getLocInRingAsPort(key).equals(serverPort)){
                Log.d(TAG, "insert: Location to store key in the ring is in this AVD");
                Uri retUri = myInsert(uri,values);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(MSG_REQUEST_TYPE,INSERT_REPLICA);
                jsonObject.put(KEY,key);
                jsonObject.put(VALUE,value);

                String succ = getSucc(serverPort);
                jsonObject.put(FORWARDING_PORT,succ);
                new ClientTask().executeOnExecutor(myExec, jsonObject.toString());  //replica 1

                String succSucc = getSucc(succ);
                jsonObject.put(FORWARDING_PORT,succSucc);
                new ClientTask().executeOnExecutor(myExec, jsonObject.toString());  //replica 2

                return retUri;
            }
            else{
                String forwardingPort = getLocInRingAsPort(key);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(MSG_REQUEST_TYPE,INSERT);
                jsonObject.put(KEY,key);
                jsonObject.put(VALUE,value);
                jsonObject.put(FORWARDING_PORT,forwardingPort);
                new ClientTask().executeOnExecutor(myExec, jsonObject.toString());
            }
		}catch (JSONException e) {
			e.printStackTrace();
		}

		return null;
	}

    public Uri myInsert(Uri uri, ContentValues values) {
        Log.d(TAG, "insert: Should be stored in current AVD");

        long row = database.insertWithOnConflict(KEYVALUE_TABLE_NAME, "", values, SQLiteDatabase.CONFLICT_REPLACE);
        Log.d(TAG, "insert: row: " + row);
        Uri newUri = uri;
        if (row > 0) {
            newUri = ContentUris.withAppendedId(uri, row);
            if (getContext() != null) {
                getContext().getContentResolver().notifyChange(newUri, null);
            }
        }

        return newUri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        if (getContext() != null) {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            serverPort = String.valueOf(Integer.parseInt(portStr) * 2);

            try {

                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
            }
        }
        Context context = getContext();
        KeyValueOpenHelper kVHelper = new KeyValueOpenHelper(context);
        database = kVHelper.getWritableDatabase();

        return database != null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        if(AT_SIGN.equals(selection)) {
            Log.d(TAG, "query: Reached AT_SIGN: "+serverPort);
            return myQuery(uri, selection);
        } else if(STAR_SIGN.equals(selection)){
            Log.d(TAG, "query: Reached STAR_SIGN: "+serverPort);
            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(KEY, selection);
                jsonObject.put(MSG_REQUEST_TYPE, QUERY_ALL);
                jsonObject.put(FORWARDING_PORT, serverPort);
                jsonObject.put(SENDER_PORT, serverPort);
                Log.d(TAG, "query: Executing client task");
                AsyncTask<String,String,String> a =new ClientTask();
                a.executeOnExecutor(myExec, jsonObject.toString());
                Log.d(TAG, "query: "+ a.getStatus());
                String s = a.get();
                Log.d(TAG, "query: "+ a.getStatus());
                Log.d(TAG, "query: a.get(): "+s);
                return jsonArr2MatrixCursor(new JSONArray(s));
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else{
            Log.d(TAG, "query: Reached ELSE case: "+serverPort);
            try {
                String forwardingPort = getLocInRingAsPort(selection);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(KEY, selection);
                jsonObject.put(MSG_REQUEST_TYPE, QUERY);
                jsonObject.put(FORWARDING_PORT, forwardingPort);
                jsonObject.put(SENDER_PORT, serverPort);
                Cursor cursor = myQuery(uri,selection);
                Log.d(TAG, "query: Executing client task");
                AsyncTask<String,String,String> a =new ClientTask();
                a.executeOnExecutor(myExec, jsonObject.toString());
                Log.d(TAG, "query: "+ a.getStatus());
                String s = a.get();
                Log.d(TAG, "query: "+ a.getStatus());
                Log.d(TAG, "query: a.get(): "+s);
                return jsonArr2MatrixCursor(new JSONArray(s));
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        return myQuery(uri,selection);
    }

    private Cursor myQuery(Uri uri,String selection){
        Log.d(TAG, "myQuery: " + "Uri: " + uri + ", " + "selection: " + selection);
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(KEYVALUE_TABLE_NAME);

        if (STAR_SIGN.equals(selection) || AT_SIGN.equals(selection))
            selection = "1==1";
        else
            selection = "key=" + "'" + selection + "'"; // appending the key sent to the Where clause

        Cursor cursor = queryBuilder.query(database, null, selection, null, null, null, null);
        if (getContext() != null) {
            cursor.setNotificationUri(getContext().getContentResolver(), uri);
        }
        Log.d(TAG, "myQuery: cursor: " + cursor);
        return cursor;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private JSONArray cur2Json(Cursor cursor) throws JSONException{

        JSONArray resultSet = new JSONArray();
        if(cursor.getCount() <= 0){
            return resultSet;
        }
        cursor.moveToFirst();

        while (!cursor.isAfterLast()) {
            int totalColumn = cursor.getColumnCount();
            JSONObject rowObject = new JSONObject();
            for (int i = 0; i < totalColumn; i++) {
                if (cursor.getColumnName(i) != null) {
                    rowObject.put(cursor.getColumnName(i),
                            cursor.getString(i));
                }
            }
            resultSet.put(rowObject);
            cursor.moveToNext();
        }

        return resultSet;

    }

    private JSONArray concatArray(JSONArray arr1, JSONArray arr2)
            throws JSONException {
        JSONArray result = new JSONArray();
        for (int i = 0; i < arr1.length(); i++) {
            result.put(arr1.get(i));
        }
        for (int i = 0; i < arr2.length(); i++) {
            result.put(arr2.get(i));
        }
        return result;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getIDfromPort(String s){
        return String.valueOf(Integer.parseInt(s) / 2);
    }

    private String getPortFromID(String s){
        return String.valueOf(Integer.parseInt(s)*2);
    }

    private String getLocInRingAsPort(String key){
        String firstID = ringFormation.getFirst();
        Iterator<String> it =  ringFormation.iterator();
        String curr = "";
        String prev = it.next();
        while(it.hasNext()){
            curr = it.next();
            System.out.println("prev: "+prev+" curr: "+curr);
            try {
                if(genHash(key).compareTo(genHash(prev)) > 0 && genHash(key).compareTo(genHash(curr)) < 0){
                    return getPortFromID(curr);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            prev = curr;
        }
        return getPortFromID(firstID);
    }

    private String getSucc(String id){
        id = getIDfromPort(id);
        String sendID = ringFormation.getFirst();
        Iterator<String> it = ringFormation.iterator();
        while(it.hasNext()){
            String curr = it.next();
            if (curr.equals(id)) {
                if (it.hasNext())
                    sendID = it.next();
            }
        }
        return getPortFromID(sendID);
    }
    private String getPred(String id){
        id = getIDfromPort(id);
        String sendID = ringFormation.getLast();
        Iterator<String> it = ringFormation.iterator();
        String prev = it.next();
        String curr = "";
        while(it.hasNext()){
            curr = it.next();
            if (curr.equals(id)) {
                sendID = prev;
            }
            prev = curr;
        }
        return getPortFromID(sendID);
    }


    private MatrixCursor jsonArr2MatrixCursor(JSONArray jsonArray){
        String[] columnNames = {"key","value"};
        MatrixCursor mc = new MatrixCursor(columnNames);

        try {
            for(int i=0 ; i < jsonArray.length(); i++ ) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                mc.addRow(new String[]{jsonObject.getString("key"),jsonObject.getString("value")});
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }


        return mc;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String m = input.readLine();

                    JSONObject msgJsonObj = new JSONObject(m);
                    Log.d(TAG, "*******************NEW REQUEST************************");
                    Log.d(TAG, "doInBackground: received msgJsonObj: "+msgJsonObj);

                    String msg_request_type = msgJsonObj.getString(MSG_REQUEST_TYPE);
                    Log.d(TAG, "doInBackground: msg_request_type: "+msg_request_type);

                    if(INSERT.equals(msg_request_type)){
                        ContentValues contentValues = new ContentValues();
                        contentValues.put(KEY,msgJsonObj.getString(KEY));
                        contentValues.put(VALUE,msgJsonObj.getString(VALUE));
                        insert(Uri.parse(PROVIDER_URI),contentValues);

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(SUCCESS);
                        Log.d(TAG, "doInBackground: Writing back INSERT SUCCESS from: "+serverPort);
                    }
                    else if(INSERT_REPLICA.equals(msg_request_type)){
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key",msgJsonObj.getString(KEY));
                        contentValues.put("value",msgJsonObj.getString(VALUE));
                        myInsert(Uri.parse(PROVIDER_URI), contentValues);

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(SUCCESS);
                        Log.d(TAG, "doInBackground: Writing back INSERT_REPLICA SUCCESS from: "+serverPort);

                    } else if(QUERY_ALL.equals(msg_request_type)){
                        Log.d(TAG, "doInBackground: ServerTask: "+serverPort);

                        JSONArray retJsonArr = new JSONArray();
                        JSONArray jsonArray;

                        for(String emulID: ringFormation){
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(KEY,msgJsonObj.getString(KEY));
                            jsonObject.put(MSG_REQUEST_TYPE, QUERY);
                            jsonObject.put(FORWARDING_PORT, getPortFromID(emulID));
                            jsonObject.put(SENDER_PORT,msgJsonObj.getString(SENDER_PORT));

                            Log.d(TAG, "doInBackground: Inside IF condition");
                            if(!getPortFromID(emulID).equals(serverPort)) {
                                Socket socketForw = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                                PrintWriter out = new PrintWriter(socketForw.getOutputStream(), true);
                                out.println(jsonObject);
                                Log.d(TAG, "doInBackground: Writing " + jsonObject + " to " + jsonObject.getString(FORWARDING_PORT));


                                BufferedReader retInput = new BufferedReader(new InputStreamReader(socketForw.getInputStream()));
                                String retValue = retInput.readLine();
                                Log.d(TAG, "doInBackground: Read value: " + retValue);
                                jsonArray = new JSONArray(retValue);
                                Log.d(TAG, "doInBackground: Converting to jsonArray: " + jsonArray);
                            }
                            else{
                                Log.d(TAG, "doInBackground: Querying local AVD ");
                                Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),msgJsonObj.getString(KEY));
                                jsonArray = cur2Json(cursor);
                            }

                            retJsonArr = concatArray(jsonArray,retJsonArr);
                            Log.d(TAG, "doInBackground: mergedJSONArray: "+retJsonArr);
                        }

                        Log.d(TAG, "doInBackground: Outside FOR loop");
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        Log.d(TAG, "doInBackground: JSON_ARRAY: "+retJsonArr);
                        out.println(retJsonArr.toString());

                        Log.d(TAG, "doInBackground: Writing back from: "+serverPort);

                    } else if(QUERY.equals(msg_request_type)){

                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        Cursor cursor = myQuery(Uri.parse(PROVIDER_URI),msgJsonObj.getString(KEY));
                        Log.d(TAG, "doInBackground: JSON_ARRAY: "+cur2Json(cursor).toString());
                        out.println(cur2Json(cursor).toString());
                        Log.d(TAG, "doInBackground: Writing back from: "+serverPort);

                    }

                } catch (IOException e) {
                    Log.d(TAG, e.toString());
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }

            //return null;
        }

        protected void onProgressUpdate(String... msg) {
            /*
             * The following code displays what is received in doInBackground().
             */
            if(msg.length == 1) {
                String m = msg[0];
                Log.d(TAG, "onProgressUpdate: String: " + m);
                new ClientTask().executeOnExecutor(myExec, m);
            }


        }
    }

    private class ClientTask extends AsyncTask<String, String, String> {
        private final String TAG = this.getClass().getSimpleName();

        @Override
        protected String doInBackground(String... msgs) {
            String m = msgs[0];
            Log.d(TAG, "doInBackground: This is client " + m);
            try {
                JSONObject jsonObject = new JSONObject(m);
                String request_type = jsonObject.getString(MSG_REQUEST_TYPE);

                if(INSERT.equals(request_type) || INSERT_REPLICA.equals(request_type)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(m);
                    Log.d(TAG, "doInBackground: Writing "+m+" to "+jsonObject.getString(FORWARDING_PORT));

                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String retValue = input.readLine();
                    Log.d(TAG, "doInBackground: Returned value from Server [" + jsonObject.getString(FORWARDING_PORT) + "]" + " is: " + retValue);
                    return retValue;

                } else if(QUERY_ALL.equals(request_type)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(m);

                    Log.d(TAG, "doInBackground: Writing "+m+" to "+jsonObject.getString(FORWARDING_PORT));



                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String retValue = input.readLine();
                    Log.d(TAG, "doInBackground: Returned value from Server [" + jsonObject.getString(FORWARDING_PORT) + "]" + " is: " + retValue);
                    //input.close();
                    //socket.close();
                    JSONArray jsonArray = new JSONArray(retValue);
                    Log.d(TAG, "doInBackground: Converting to jsonArray: "+jsonArray);

                    return jsonArray.toString();

                } else if(QUERY.equals(request_type)){
                    Log.d(TAG, "doInBackground: Sending REQUEST_TYPE: " + request_type + " to: " + jsonObject.getString(FORWARDING_PORT));

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(jsonObject.getString(FORWARDING_PORT)));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(m);

                    Log.d(TAG, "doInBackground: Writing " + m + " to " + jsonObject.getString(FORWARDING_PORT));


                    BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String retValue = input.readLine();
                    Log.d(TAG, "doInBackground: Returned value from Server [" + jsonObject.getString(FORWARDING_PORT) + "]" + " is: " + retValue);
                    return retValue;

                }


            } catch (JSONException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
