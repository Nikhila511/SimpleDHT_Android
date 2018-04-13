package edu.buffalo.cse.cse486586.simpledht;

// References : https://developer.android.com/reference/android/database/MatrixCursor.html
// REferences : https://developer.android.com/reference/android/database/sqlite/

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    HelperDb msgHelperDb;
    String present_port;
    private static ArrayList<String> chord_ring = new ArrayList();
    private static ArrayList<String> node_hashes = new ArrayList<String>();
    private String my_hashval = "";
    String predecessor_hashval = "";
    String successor_hashval = "";
    int predecessor_port = 0;
    int successor_port = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String dbTable = "msgTable";
        String delete_key = selection;
        String delete_key_hashval = null;
        try{
            delete_key_hashval = genHash(delete_key);
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        Log.v(TAG, "Delete request for : "+delete_key);
        int rows_count = 0;
        if(predecessor_port== 0 && successor_port == 0) {
            // only single avd running
            if (delete_key.equals("*") || delete_key.equals("@")) {

                // Delete all contents in this avd
                // int rows = msgHelperDb.getWritableDatabase().delete(dbTable,"key =",new String[]{selection});
                int rows = msgHelperDb.getWritableDatabase().delete(dbTable,null, null);
                return rows;
            } else {

                int rows = msgHelperDb.getWritableDatabase().delete(dbTable,"key =?",new String[]{selection});
                return rows;
            }
        }else if(selection.equals("@")){

            // Delete one avd's content on selecting @
            int rows = msgHelperDb.getWritableDatabase().delete(dbTable,null, null);
            return rows;

        }else if(selection.equals("*")){

            Request req = new Request();
            req.setAction("DELETE");
            req.setPresent_port(present_port);
            req.setNeighbors("*");
            try {
                if (selectionArgs == null) {

                    Log.v(TAG, "found delete arg * with selection args null at port : "+ req.getPresent_port());
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2);
                    int rows = msgHelperDb.getWritableDatabase().delete(dbTable,null, null);
                    return rows;

                } else {
                    String from_port = selectionArgs[0];
                    Log.v(TAG, "from port is : "+from_port);
                    if (!from_port.equals(Integer.toString(successor_port))) {

                        Log.v(TAG, "Dint reach end of ring");
                        Request req1 = new Request();
                        req1.setAction("DELETE");
                        req1.setPresent_port(from_port);
                        req1.setNeighbors("*");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req1, successor_port * 2);

                    }
                    int rows = msgHelperDb.getWritableDatabase().delete(dbTable,null, null);
                    return rows;
                }
            }catch(Exception e){
                e.printStackTrace();
            }

        }else if(delete_key_hashval.compareTo(predecessor_hashval) >= 0 && delete_key_hashval.compareTo(my_hashval) < 0){
            int p = 5554 + node_hashes.indexOf(my_hashval)*2;
            Log.v(TAG, "DEleting at : "+ Integer.toString(p));

            int rows = msgHelperDb.getWritableDatabase().delete(dbTable,"key =?",new String[]{selection});
            return rows;
        }else if(predecessor_hashval.compareTo(my_hashval) > 0 && successor_hashval.compareTo(my_hashval) > 0){
            // Delete req at first node
            int p = 5554 + node_hashes.indexOf(my_hashval)*2;
            Log.v(TAG, "Delete req at first node at : "+ Integer.toString(p));
            if((delete_key_hashval.compareTo(predecessor_hashval) >= 0 || delete_key_hashval.compareTo(my_hashval) < 0)){

                int rows = msgHelperDb.getWritableDatabase().delete(dbTable,"key =?",new String[]{selection});
                return rows;

            }else{
                try {
                    Request req = new Request();
                    req.setAction("DELETE");
                    req.setPresent_port(present_port);
                    req.setNeighbors(selection);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }else {
            // pass the request onto the successor
            try {
                Request req = new Request();
                req.setAction("DELETE");
                req.setPresent_port(present_port);
                req.setNeighbors(selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String dbTable = "msgTable";
        String key_hashval= "";
        String key = values.getAsString("key");
        try{
            key_hashval = genHash(key);
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }

        if(successor_port == 0 && predecessor_port == 0){
            // Single node in the ring so insert everything into the same.
            try {
                msgHelperDb.getWritableDatabase().insertWithOnConflict(dbTable, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                msgHelperDb.getWritableDatabase().close();
                return uri;
            }catch(SQLException e){
                e.printStackTrace();
            }
        }else if(key_hashval.compareTo(predecessor_hashval) >=0 && my_hashval.compareTo(key_hashval) >0){
            // key val is between pred and present node hash. hence insert contents to own node.
            try {
                int p = 5554 + node_hashes.indexOf(my_hashval)*2;
                Log.v(TAG,"inserting key : "+key+" at avd: "+Integer.toString(p));
                msgHelperDb.getWritableDatabase().insertWithOnConflict(dbTable, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                msgHelperDb.getWritableDatabase().close();
                return uri;
            }catch(SQLException se){
                se.printStackTrace();
            }
        }else if(my_hashval.compareTo(predecessor_hashval) < 0 && my_hashval.compareTo(successor_hashval)<0){
            if(key_hashval.compareTo(predecessor_hashval) >= 0 || key_hashval.compareTo(my_hashval) < 0){
                try {
                    int p = 5554 + node_hashes.indexOf(my_hashval)*2;
                    Log.v(TAG,"inserting key : "+key+" at avd: "+Integer.toString(p));
                    msgHelperDb.getWritableDatabase().insertWithOnConflict(dbTable, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    msgHelperDb.getWritableDatabase().close();
                    return uri;
                }catch(SQLException e){
                    e.printStackTrace();
                }
            }else{
                String content = "key#" + values.getAsString("key") + "#value#" + values.getAsString("value");
                Request req = new Request();
                req.setAction("INSERT");
                req.setPresent_port(present_port);
                req.setNeighbors(content);
                int s_p = 5554 + node_hashes.indexOf(successor_hashval)*2;
                Log.v(TAG,"forwarding key : "+key+" at successor "+Integer.toString(s_p));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2);
            }
        }else{
            String content = "key#" + values.getAsString("key") + "#value#" + values.getAsString("value");
            Request req = new Request();
            req.setAction("INSERT");
            req.setPresent_port(present_port);
            req.setNeighbors(content);
            int s_p = 5554 + node_hashes.indexOf(successor_hashval)*2;
            Log.v(TAG,"forwarding key : "+key+" at successor "+Integer.toString(s_p));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2);
        }
        Log.v("insert", values.toString());
        return null;
    }

    @Override
    public boolean onCreate() {
        msgHelperDb = new HelperDb(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        String port = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        present_port = port;
        try{
            my_hashval = genHash(port);
        }catch(Exception e){
            e.printStackTrace();
        }
        for(int p = 5554; p <= 5562; p=p+2){
            try{
                node_hashes.add(genHash(Integer.toString(p)));
            }catch(NoSuchAlgorithmException e){
                e.printStackTrace();
            }
        }
        Log.v("list : ","node hashes is ");
        for(int i = 0; i<node_hashes.size();i++){
            Log.v("at i ", i + " "+node_hashes.get(i));
        }
        try{
            ServerSocket server_socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
        }catch(SocketException se){
            se.printStackTrace();
        }catch(IOException ie){
            ie.printStackTrace();
        }
        try{
            // initiate join request to central joining posrt that is 5554
            if(!port.equals("5554")){
                Request req_join = new Request();
                req_join.setAction("JOIN");
                req_join.setPresent_port(port);
                req_join.setNeighbors("TOBESET");
                Log.v("Joining new avd : ",port+" req string: "+ req_join.toString());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req_join, 11108);
            }else {
                chord_ring.add(genHash(port));
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        String dbTable = "msgTable";
        String query_key = selection;
        String query_key_hashval = null;
        try{
            query_key_hashval = genHash(query_key);
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        Cursor result_cursor = null;
        Log.v(TAG, "Querying for : "+query_key);
        if(predecessor_port== 0 && successor_port == 0) {
            // only single avd running
            if (selection.equals("*") || selection.equals("@")) {
                // return all the contents on this single avd
                Cursor cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
                result_cursor = cursor;
            } else {
                Cursor entry = msgHelperDb.getReadableDatabase().query(dbTable, new String[]{"key", "value"},
                        "key=?", new String[]{selection}, null, null, null);
                result_cursor = entry;
            }
        }else if(selection.equals("@")){
            // query at any node with @, return local db cursor.
            Cursor cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
            result_cursor = cursor;
        }else if(selection.equals("*")){
            Request req = new Request();
            req.setAction("QUERY");
            req.setPresent_port(present_port);
            req.setNeighbors("*");              // acts like query request type
            try {
                //MatrixCursor all_cursors = new MatrixCursor(new String[]{"key", "value"});
                if (selectionArgs == null) {
                    Log.v(TAG, "found * with selection args null at port : "+ req.getPresent_port());
                    String contents = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2).get();
                    MatrixCursor all_cursors = new MatrixCursor(new String[]{"key", "value"});
                    all_cursors = get_all_contents(contents, all_cursors);
                    Cursor my_cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
                    my_cursor.moveToFirst();
                    while (!my_cursor.isAfterLast()) {
                        Object[] values = {my_cursor.getString(0), my_cursor.getString(1)};
                        my_cursor.moveToNext();
                        all_cursors.addRow(values);
                    }
                    result_cursor = all_cursors;
                } else {
                    MatrixCursor all_cursors = new MatrixCursor(new String[]{"key", "value"});
                    String from_port = selectionArgs[0];
                    Log.v(TAG, "from port is : "+from_port);
                    if (!from_port.equals(Integer.toString(successor_port))) {
                        Log.v(TAG, "Dint reach end of ring");
                        Request req1 = new Request();
                        req1.setAction("QUERY");
                        req1.setPresent_port(from_port);
                        req1.setNeighbors("*");
                        String contents = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req1, successor_port * 2).get();
                        all_cursors = get_all_contents(contents, all_cursors);

                    }
                    Cursor my_cursor = msgHelperDb.getReadableDatabase().rawQuery("Select * from " + dbTable, null);
                    my_cursor.moveToFirst();
                    while (!my_cursor.isAfterLast()) {
                        Object[] values = {my_cursor.getString(0), my_cursor.getString(1)};
                        my_cursor.moveToNext();
                        all_cursors.addRow(values);
                    }
                    result_cursor = all_cursors;

                }
            }catch(Exception e){
                e.printStackTrace();
            }

        }else if(query_key_hashval.compareTo(predecessor_hashval) >= 0 && query_key_hashval.compareTo(my_hashval) < 0){
            int p = 5554 + node_hashes.indexOf(my_hashval)*2;
            Log.v(TAG, "Querying at : "+ Integer.toString(p));
            Cursor entry = msgHelperDb.getReadableDatabase().query(dbTable, null,
                    "key=?", new String[]{selection}, null, null, null);
            Log.v("QUERY"," for key:"+selection+" found at port: "+Integer.toString(p));
            result_cursor = entry;
        }else if(predecessor_hashval.compareTo(my_hashval) > 0 && successor_hashval.compareTo(my_hashval) > 0){
            // corner case at smalles node query.
            int p = 5554 + node_hashes.indexOf(my_hashval)*2;
            Log.v(TAG, "Querying at : "+ Integer.toString(p));
            if((query_key_hashval.compareTo(predecessor_hashval) >= 0 || query_key_hashval.compareTo(my_hashval) < 0)){
                Cursor entry = msgHelperDb.getReadableDatabase().query(dbTable, null,
                        "key=?", new String[]{selection}, null, null, null);
                result_cursor = entry;
            }else{
                try {
                    Request req = new Request();
                    req.setAction("QUERY");
                    req.setPresent_port(present_port);
                    req.setNeighbors(selection);
                    int suc_p = 5554 + node_hashes.indexOf(successor_hashval)*2;
                    Log.v(TAG, "Sending req to successor for querying key: "+query_key+" at suc port : "+ Integer.toString(suc_p));
                    String contents = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2).get();
                    Log.v(TAG, "Sending req to successor for querying key: "+query_key+" at suc port : "+ Integer.toString(suc_p)+" query key received from succ as : "+contents);
                    MatrixCursor m_cursor = get_response(contents);
                    result_cursor = m_cursor;
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }else {
            // pass the request onto the successor
            try {
                Request req = new Request();
                req.setAction("QUERY");
                req.setPresent_port(present_port);
                req.setNeighbors(selection);
                int suc_p = 5554 + node_hashes.indexOf(successor_hashval)*2;
                Log.v(TAG, "Sending req to successor for querying key: "+query_key+" at suc port : "+ Integer.toString(suc_p));
                String contents = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, successor_port * 2).get();
                Log.v(TAG, "Sending req to successor for querying key: "+query_key+" at suc port : "+ Integer.toString(suc_p)+" query key received from succ as : "+contents);
                MatrixCursor m_cursor = get_response(contents);
                result_cursor = m_cursor;
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result_cursor;
    }
    public MatrixCursor get_response(String data){
        MatrixCursor cursor = new MatrixCursor(new String[]{"key","value"});
        try {
            if(data!=null) {
                JSONObject resp = new JSONObject(data);
                Object[] values = {resp.getString("key"), resp.getString("value")};
                cursor.addRow(values);
            }
        }catch(JSONException e){
            e.printStackTrace();
        }
        return cursor;
    }
    public MatrixCursor get_all_contents(String data, MatrixCursor m_cursor){
        try {
            Log.v(TAG, "data sent to get_all_contents is: "+ data);
            JSONObject jsonObject = new JSONObject(data);
            JSONArray all_keys = jsonObject.getJSONArray("keys");
            JSONArray all_values = jsonObject.getJSONArray("values");

            int i=0;
            while(i < all_keys.length()){
                Object[] content_pairs = {all_keys.getString(i), all_values.getString(i)};
                m_cursor.addRow(content_pairs);
                i++;
            }
        }catch(JSONException e){
            e.printStackTrace();
        }
        return m_cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket s;
            try{
                while(true) {
                    s = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                    String request_string = in.readLine();
                    Log.v(TAG, "Inside server task");
                    while(request_string==null){
                        request_string=in.readLine();
                    }
                    Request req = new Request();
                    Log.v(TAG,"Listening on server task");
                    Log.v(TAG,"server Task| my request: "+request_string);
                    String[] req_params = request_string.split(":");
                    for (int k = 0; k < req_params.length; k++) {
                        if (req_params[k].equals("action")) {
                            k = k + 1;
                            req.action = req_params[k];
                            continue;
                        } else if (req_params[k].equals("port")) {
                            k++;
                            req.present_port = req_params[k];
                            continue;
                        } else if (req_params[k].equals("neighbors")) {
                            k++;
                            req.neighbors = req_params[k];
                            continue;
                        }
                    }
                    if(!req.getAction().equals("QUERY")){
                        s.close();
                        s = serverSocket.accept();
                        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                        out.write("Write OK");
                        out.flush();
                        out.close();
                        s.close();
                    }

                    if(req.getAction().equals("INSERT")){
                        ContentValues content_values = new ContentValues();
                        String[] key_value = req.getNeighbors().split("#");
                        content_values.put("key",key_value[1]);
                        content_values.put("value",key_value[3]);
                        insert(null,content_values);
                        s.close();
                    }else if(req.getAction().equals("QUERY")){
                        String my_query = req.getNeighbors();
                        String data = "";
                        Log.v(TAG,"servertask | when action is QUERY opwnfor port : "+present_port+"my query is : "+my_query);
                        if(my_query.equals("*")){
                            Log.v(TAG," Found * in serever task at port : "+ present_port);
                            Cursor cursor = query(null, null, "*", new String[]{req.getPresent_port()}, null);
                            data = getFinalContents(cursor);
                            Log.v(TAG, "data sttring written is : "+data);

                        }else {
                            Cursor content_point = query(null,null,my_query,null,null);
                            Log.v("Query","single key | Cursor Object"+ DatabaseUtils.dumpCursorToString(content_point));
                            if(content_point!=null && content_point.moveToFirst()) {
                                JSONObject content = new JSONObject();
                                content.put("key", content_point.getString(content_point.getColumnIndex("key")));
                                content.put("value", content_point.getString(content_point.getColumnIndex("value")));
                                data = content.toString();
                                Log.v("Query","single key | Cursor string sending back :"+data);
                                content_point.close();
                            }

                        }
                        Log.v(TAG," closing socket");
                        s = serverSocket.accept();
                        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                        out.write(data);
                        out.flush();
                        out.close();
                        s.close();

                    }else if(req.getAction().equals("DELETE")){

                        String my_delete_key = req.getNeighbors();
                        Log.v(TAG,"servertask | when action is DELETE opwnfor port : "+present_port+"my delete key is : "+my_delete_key);
                        if(my_delete_key.equals("*")){

                            Log.v(TAG," Found * in serever task at port : "+ present_port);
                            delete(null,"*",new String[]{req.getPresent_port()});

                        }else {

                            delete(null,my_delete_key,null);
                        }

                    }else if(req.getAction().equals("JOIN")){
                        String new_node = req.getPresent_port();
                        Log.v("Servertask:", "JOIN for port : "+new_node);
                        String new_node_hashval = genHash(new_node);
                        chord_ring.add(new_node_hashval);
                        Collections.sort(chord_ring);
                        Log.v("Chord ring: ","sorted");
                        for(int i=0;i<chord_ring.size();i++){
                            int chord_el = 5554+node_hashes.indexOf(chord_ring.get(i))*2;
                        }
                        // set the node's predecessor and successor
                        String node_neighbors = "";
                        if(chord_ring.indexOf(new_node_hashval)==0){
                            node_neighbors = "Successor#"+ chord_ring.get(1) +"#Predecessor#"+ chord_ring.get(chord_ring.size()-1);
                        }else if(chord_ring.indexOf(new_node_hashval)==chord_ring.size()-1){
                            node_neighbors = "Successor#"+ chord_ring.get(0) +"#Predecessor#"+ chord_ring.get(chord_ring.size()-2);
                        }else{
                            // if not above, access its neighbors and set the predecessor and successor
                            int successor = (chord_ring.indexOf(new_node_hashval)+1);
                            int predecessor = (chord_ring.indexOf(new_node_hashval)-1);
                            node_neighbors = "Successor#"+ chord_ring.get(successor) +"#Predecessor#"+ chord_ring.get(predecessor);
                        }
                        //
                        Log.v("my node: ",Integer.toString(5554+node_hashes.indexOf(new_node_hashval)*2));
                        Log.v("neighbors : ",node_neighbors);
                        String[] nei = node_neighbors.split("#");
                        int suc = 5554 + node_hashes.indexOf(nei[1])*2;
                        int pred = 5554 + node_hashes.indexOf(nei[3])*2;
                        Log.v(TAG, "successor is : "+ Integer.toString(suc) + " predecessor is : "+ Integer.toString(pred));
                        req.setNeighbors(node_neighbors);
                        req.setAction("setMyNeighbors");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req, Integer.parseInt(req.getPresent_port()) * 2);

                        // changing new_node's successor's predecessor.
                        Request req_suc = new Request();
                        req_suc.setAction("setPredecessor");
                        req_suc.setPresent_port(present_port);
                        req_suc.setNeighbors(req.getPresent_port());
                        String[] successor_predecessor = req.getNeighbors().split("#");
                        Integer suc_port = 5554 + node_hashes.indexOf(successor_predecessor[1])*2;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req_suc, suc_port * 2);

                        // Changing new_node's predecessor's successor
                        Request req_pred = new Request();
                        req_pred.setAction("setSuccessor");
                        req_pred.setPresent_port(present_port);
                        req_pred.setNeighbors(req.getPresent_port());
                        Integer pred_port = 5554 + node_hashes.indexOf(successor_predecessor[3])*2;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, req_pred, pred_port * 2);
                        s.close();

                    }else if(req.getAction().equals("setMyNeighbors")){
                        Log.v(TAG,"Servertask || set neighbors for : "+ req.getPresent_port());
                        String[] successor_predecessor = req.getNeighbors().split("#");
                        successor_hashval = successor_predecessor[1];
                        predecessor_hashval = successor_predecessor[3];
                        successor_port = 5554 + node_hashes.indexOf(successor_hashval)*2;
                        predecessor_port = 5554 + node_hashes.indexOf(present_port)*2;
                        s.close();
                    }else if(req.getAction().equals("setPredecessor")){
                        Log.v(TAG,"Servertask || set predecessor for : "+ req.getPresent_port());

                        predecessor_hashval = genHash(req.getNeighbors());
                        predecessor_port = Integer.parseInt(req.getNeighbors());
                        s.close();
                    }else if(req.getAction().equals("setSuccessor")){
                        Log.v(TAG,"Servertask || set Successor for : "+ req.getPresent_port());

                        successor_hashval = genHash(req.getNeighbors());
                        successor_port = Integer.parseInt(req.getNeighbors());
                        s.close();
                    }
                    //s.close();
                }

            }catch(Exception e){
                Log.e(TAG, "ServerTask Exception");
                e.printStackTrace();
            }
            return null;
        }


    }
    public String getFinalContents(Cursor c){
        JSONObject data_json = new JSONObject();
        try {
            JSONArray all_keys = new JSONArray();
            JSONArray all_values = new JSONArray();
            if(c!=null) {
                c.moveToFirst();
                int i = 0;
                while (!c.isAfterLast()) {
                    all_keys.put(i, c.getString(c.getColumnIndex("key")));
                    all_values.put(i, c.getString(c.getColumnIndex("value")));
                    i++;
                    c.moveToNext();
                }
                data_json.put("keys", all_keys);
                data_json.put("values", all_values);
            }
        }catch(JSONException e){
            e.printStackTrace();
        }catch (Exception e1){
            e1.printStackTrace();
        }
        return data_json.toString();
    }

    private class ClientTask extends AsyncTask<Object, Void, String>{
        @Override
        protected String doInBackground(Object... msg) {
            Request req = (Request) msg[0];
            int myPort =   (Integer) msg[1];
            Log.v("client task for: ",Integer.toString(myPort));
            Log.v("client task for: ",req.toString());
            Socket socket;
            Socket socket1;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        myPort);
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                out.write(req.toString());
                Log.v(TAG, " wrote to out at port: "+ myPort);
                out.flush();
                out.close();
                socket.close();
                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        myPort);
                //socket1.setSoTimeout(5000);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
                String data = in.readLine();
                Log.v(TAG,"received data");
                if(data == null) {
                    socket1.close();
                    return null;
                }
                if (data.equals("Write OK")) {
                    Log.v(TAG,"received write OK");
                    //out.close();
                    socket1.close();
                } else {
                    Log.v(TAG,"received something else");
                    //out.close();
                    socket1.close();
                    return data;
                }
            }catch (IOException e) {
                Log.d(TAG,"Caught IOException for "+ myPort);
                e.printStackTrace();
                return null;
            }catch(Exception e){
                Log.e(TAG,"caught client exception");
                e.printStackTrace();
            }
            return null;
        }
    }
}