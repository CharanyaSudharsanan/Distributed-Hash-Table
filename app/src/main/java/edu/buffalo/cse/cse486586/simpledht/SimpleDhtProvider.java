package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


//references - Oracle docs, Android developers.com

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    public String c_port = "";
    public String c_hash = "";
    public String c_succ = "";
    public String c_pred = "";
    public String c_pred_hash = "";
    public String c_succ_hash = "";
    public String hash_key;
    public String h_key;
    public HashSet<String> set = new HashSet<String>();

    public String myPort;
    static final String[] ports = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    static final String[] portid ={"5554","5556","5558","5560","5562"};
    public static HashMap<String,String> avd_id= new HashMap<String, String>();
    public static HashMap<String,String> query_check = new HashMap<String, String>();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	public BlockingQueue<String> queue;//REferences for syntaxes - Oraclejava, Android developers
	public BlockingQueue<String> squeue;
    public StringBuilder sbuf = new StringBuilder();
    ArrayList<Node> nodes = new ArrayList<Node>();



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String k = selection;
        String h = "";
        File dir = getContext().getFilesDir();
            try {
                    h = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }


        if(k.equals("@") ){
            if (dir.exists()) {
                File[] files = dir.listFiles();
                Log.d("@Delete@","Total number of files to be deleted: "+files.length);
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    String strFileName = file.getName();
                    Log.d("@Delete@","File : "+strFileName);
                    getContext().deleteFile(strFileName);
                    Log.d("@Delete@","Success");
                }
            }
            if(selectionArgs != null){
                if (dir.exists()) {
                File[] files = dir.listFiles();
                Log.d("@Delete@","Total number of files to be deleted: "+files.length);
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    String strFileName = file.getName();
                    Log.d("@Delete@","File : "+strFileName);
                    getContext().deleteFile(strFileName);
                    Log.d("@Delete@","Success");
                }
                }
                String imsg = String.format("*del*:%s:%s", c_succ,selectionArgs[0]);
                if(!(selectionArgs[0].equals(c_port))){
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                }
            }
        }

        else if(k.equals("*"))
        {
            if (dir.exists()) {
                File[] files = dir.listFiles();
                Log.d("@Delete@","Total number of files to be deleted: "+files.length);
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    String strFileName = file.getName();
                    Log.d("@Delete@","File : "+strFileName);
                    getContext().deleteFile(strFileName);
                    Log.d("@Delete@","Success");
                }
            }

            //call clienttask with c_port
            String imsg = String.format("*del*:%s:%s", c_succ,c_port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
        }

        else {
                if(nodes.size() == 1){

                    if (dir.exists()) {
                    File[] files = dir.listFiles();
                    Log.d("@Delete@","Total number of files to be deleted: "+files.length);
                    getContext().deleteFile(selection);
                    Log.d("@Delete@","Success");
                        }
                    }


                else if(nodes.size() > 1)
                {

                Log.d("Delete", "Selection_Value" + selection);
                Log.d("Delete", "Hash to be deleted: " + h);
                Log.d("Delete:", "current : " + c_port + " hash : " + h);
                Log.d("Delete:", "predecessor : " + c_pred + " hash : " + c_pred_hash);
                //non-edge case
                if (c_hash.compareTo(c_pred_hash) > 0) {
                    Log.d("Delete", "Entering NonEdge Case");
                    Log.d("Delete : ", "h_key" + h);
                        if ((c_hash.compareTo(h) > 0) && (c_pred_hash.compareTo(h) < 0)) {
                            //belongs to curr node
                            Log.d("Delete", "Belongs to current node!Performs delete!");
                            getContext().deleteFile(selection);
                        } else {
                            String imsg = String.format("*del_specific*:%s:%s", c_succ,selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                        }
                }
            //edge case
                else {
                    if ((c_hash.compareTo(h) > 0) || (c_pred_hash.compareTo(h) < 0)) {
                        getContext().deleteFile(selection);
                    } else {
                        String imsg = String.format("*del_specific*:%s:%s", c_succ, selection);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                    }

                }
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
        String filename = values.getAsString("key");
        String string = values.getAsString("value");

        Log.d("string to be stored",string);

        try {
             hash_key = genHash(filename);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.d("Hash to be routed",hash_key);

        if(nodes.size() == 1)
        {
            Log.d("insert","Node Size : 1");

            FileOutputStream outputStream;
                try {
                    Log.d("insert","Write Starts"+string);
                    outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                    outputStream.write(string.getBytes());
                    Log.d("insert","write successful");
                    Log.d("Finalwrite","Stored at node :"+c_port+"Message stored:"+string+"hashkey"+hash_key);
                    outputStream.close();
                } catch (Exception e) {
                    Log.e("Insert", "File write failed");
                }
                Log.v("insert", values.toString());
        }
        else if(nodes.size() > 1)
        {
            Log.d("insert","Node Size :"+nodes.size());
            //non-edge case
            Log.d("insert:","current:"+c_port+ "hash" +c_hash);
            Log.d("insert:","current:"+c_pred+"hash"+c_pred_hash);

            if (c_hash.compareTo(c_pred_hash) > 0) {
                Log.d("insert","Entering NonEdge Case");
                if ((c_hash.compareTo(hash_key) > 0) && (c_pred_hash.compareTo(hash_key) < 0)) {
                    //belongs to curr node
                    Log.d("insert","Belongs to current node!Performs insert!");
                    FileOutputStream outputStream;
                    try {
                        Log.d("insert", "Write Starts" + string);
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(string.getBytes());
                        Log.d("insert", "write successful");
                        Log.d("Finalwrite","Stored at node :"+c_port+"Message stored:"+string+"hashkey"+hash_key);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e("Insert", "File write failed at node"+c_hash);
                    }
                    Log.v("insert", values.toString());
                }
                //doesnt belong to your node. route!!
                else{
                    Log.d("insert","Routing!!!!!!!!!!!!!!!!!!!"+c_succ);
                    String imsg = String.format("succ:%s:%s:%s:%s", c_succ, c_succ_hash, filename, string);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                }

            }
            //edge case
            else {
                Log.d("insert","Entering Edge Case");

                //belongs to curr node
                if ((c_hash.compareTo(hash_key) > 0) || (c_pred_hash.compareTo(hash_key) < 0)) {
                Log.d("insert","Belongs to current node!Performs insert!");
                    FileOutputStream outputStream;
                    try {
                        Log.d("insert", "Write Starts" + string);
                        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                        outputStream.write(string.getBytes());
                        Log.d("insert", "write successful");
                        Log.d("Finalwrite","Stored at node :"+c_port+"Message stored:"+string+"hashkey"+hash_key);
                        outputStream.close();
                    } catch (Exception e) {
                        Log.e("Insert", "File write failed");
                    }
                    Log.v("insert", values.toString());
                }
                //doesnt belong to curr node, route!
                else{
                    Log.d("insert","Routing!!!!!!!!!!!!!!!!!!!"+c_succ);
                    String imsg = String.format("succ:%s:%s:%s:%s", c_succ, c_succ_hash, filename, string);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                }
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d("Final","CurrentPort: "+myPort);

        avd_id.put("5554", "11108");
        avd_id.put("5556", "11112");
        avd_id.put("5558", "11116");
        avd_id.put("5560", "11120");
        avd_id.put("5562", "11124");
        c_port = portStr;

        try {
            c_hash = genHash(c_port);
        }catch (NoSuchAlgorithmException e)
        {
            Log.d(TAG, "Hash Value not generated!");
        }



        String msg = "Join:"+portStr;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch (IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
        }

        Node new_node = new Node(c_port,c_hash);
        nodes.add(new_node);
        set.add(c_port);
        new_node.setSuccesor(new_node);
        new_node.setPredecessor(new_node);

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        if(!(c_port.equals("5554"))){
                Log.d("Final","CallingClientTaskof: "+myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }

        return false;

        }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        public Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
         @Override
         protected Void doInBackground(ServerSocket... sockets) {

             while (true) {
                 try {
                     String ack="";
                     Log.d("Server", "Server is Listening");
                     ServerSocket serverSocket = sockets[0];
                     Socket sock = serverSocket.accept();

                     StringBuilder stringBuilder = new StringBuilder();
                     ContentResolver mContentResolver;
                     mContentResolver = getContext().getContentResolver();
                     Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                     DataInputStream din = new DataInputStream(sock.getInputStream());
                     DataOutputStream dout;// = new DataOutputStream(sock.getOutputStream());
                     String message = din.readUTF();
                     Log.d("Server","message recieved:"+message);

                     String[] m = message.split("\\:");
                     Log.d(TAG,"Split Messages"+m[0]+" "+m[1]);


                     if(m[0].equals("add"))
                     {
                         Log.d("Server","inside add");

                         String current_hash = "";
                         String current;

                         current = m[1];//to be added 5554
                         try {
                             current_hash = genHash(current);
                             Log.d(TAG,"Hash val :"+current_hash);
                             }
                         catch (NoSuchAlgorithmException e)
                            {
                            Log.d(TAG, "Hash Value not generated!");
                            }
                         Log.d("Server","Curr node :"+current);
                         Log.d("Server","Hash val :"+current_hash);

                         Node curr_node = new Node(current,current_hash);

                         if(!(set.contains(current))) {
                             set.add(current);
                             nodes.add(curr_node);
                             Log.d("Server","node added to dht!");
                         }

                         Log.d("Number of nodes in cset",":"+set.size());
                         Iterator<String> it = set.iterator();

                         while(it.hasNext()){
                             String p = it.next();

                             if((!p.equals(current))){
                                 stringBuilder.append(p);
                                 stringBuilder.append(":");
                             }
                         }
                         Log.d("Server-stringBuilder",stringBuilder.toString());

                         String ms = "list:" + stringBuilder.toString();
                         ms.trim();

                         dout = new DataOutputStream(sock.getOutputStream());
                         dout.writeUTF(ms);
                         dout.flush();


                         Collections.sort(nodes);

                         Log.d("Server","Node size :"+nodes.size());
                         //display nodes before adding succ n preds
//                         for(int i = 0; i < nodes.size();i++) {
//                            display_nodes(nodes.get(i));
//                         }

                         if(nodes.size() == 1)
                         {
                             Node new_nd = nodes.get(0);
                             new_nd.setSuccesor(new_nd);
                             new_nd.setPredecessor(new_nd);
                         }
                         else if(nodes.size() == 2 || nodes.size() == 3 || nodes.size() == 4 || nodes.size() == 5)
                         {
                             int n = nodes.size();
                             for(int i = 0; i < n; i++)
                             {
                                 if(i == 0) {
                                     nodes.get(i).setSuccesor(nodes.get(i + 1));
                                     nodes.get(i).setPredecessor(nodes.get(n - 1));

                                 }
                                 if(i == n-1){
                                     nodes.get(i).setSuccesor(nodes.get(0));
                                     nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if((i > 0) && (i <= (n - 2))){
                                   nodes.get(i).setSuccesor(nodes.get(i+1));
                                   nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if(nodes.get(i).port_num.equals(c_port)){
                                     c_pred = nodes.get(i).predecessor.port_num;
                                     c_succ = nodes.get(i).succesor.port_num;

                                     c_pred_hash = genHash(c_pred);
                                     c_succ_hash = genHash(c_succ);

                                     }
                                 }
                             Log.d("Closure:",":"+c_port+":"+c_hash+":"+c_succ+":"+c_pred);
                             Log.d("Closure: Node Size:",":"+nodes.size());
                             for(int j = 0; j < nodes.size();j++) {
                                 display_nodes(nodes.get(j));

                             }
                         }

                         Log.d("Server","Sorted array :");
                         //display nodes after adding succ n preds
//                         for(int i = 0; i < nodes.size();i++) {
//                            display_nodes(nodes.get(i));
//                         }

                    }
                    if(m[0].equals("broadcast")){
                         Log.d("Server","Broadcast");

                         dout = new DataOutputStream(sock.getOutputStream());
                          ack="ack";
                         ack.trim();
                         dout.writeUTF(ack);
                         dout.flush();

                        String cp = m[1];//to be added
                        String ch = "";
                         try {
                             ch = genHash(cp);
                             Log.d(TAG,"Hash val :"+ch);
                             }
                         catch (NoSuchAlgorithmException e)
                            {
                            Log.d(TAG, "Hash Value not generated!");
                            }
                         Log.d("Server","Curr node :"+cp);
                         Log.d("Server","Hash val :"+ch);
                         Node node1 = new Node(cp,ch);

                         if(!(set.contains(cp))) {
                             set.add(cp);
                             nodes.add(node1);
                             Log.d("Server","node added to dht!");
                         }

                         Collections.sort(nodes);
                         //Log.d("Server","Node size :"+nodes.size());
                         //display nodes before adding succ n preds
//                         for(int i = 0; i < nodes.size();i++) {
//                            display_nodes(nodes.get(i));
//                         }

                         if(nodes.size() == 1)
                         {
                             Node new_nd = nodes.get(0);
                             new_nd.setSuccesor(new_nd);
                             new_nd.setPredecessor(new_nd);
                         }
                         else if(nodes.size() == 2 || nodes.size() == 3 || nodes.size() == 4 || nodes.size() == 5)
                         {
                             int n = nodes.size();
                             for(int i = 0; i < n; i++)
                             {
                                 if(i == 0) {
                                     nodes.get(i).setSuccesor(nodes.get(i + 1));
                                     nodes.get(i).setPredecessor(nodes.get(n - 1));

                                 }
                                 if(i == n-1){
                                     nodes.get(i).setSuccesor(nodes.get(0));
                                     nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if((i > 0) && (i <= (n - 2))){
                                   nodes.get(i).setSuccesor(nodes.get(i+1));
                                   nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if(nodes.get(i).port_num.equals(c_port)){
                                     c_pred = nodes.get(i).predecessor.port_num;
                                     c_succ = nodes.get(i).succesor.port_num;
                                     //Log.d("Closure:",":"+c_port+":"+c_hash+":"+c_succ+":"+c_pred);
                                     c_pred_hash = genHash(c_pred);
                                     c_succ_hash = genHash(c_succ);
                                    // Log.d("Closure: Node Size:",":"+nodes.size());
//                                     for(int j = 0; j < nodes.size();j++) {
//                                         display_nodes(nodes.get(j));
//                                     }
                                 }

                                 Log.d("Closure:",":"+c_port+":"+c_hash+":"+c_succ+":"+c_pred);
                                 Log.d("Closure: Node Size:",":"+nodes.size());
                                 for(int k = 0; k < nodes.size();k++)
                                     display_nodes(nodes.get(k));{
                                    }
                             }
                         }

                         Log.d("Server","Sorted array :");
                         //display nodes after adding succ n preds
                         for(int i = 0; i < nodes.size();i++) {
                            display_nodes(nodes.get(i));
                         }
                    }
                    if(m[0].equals("del")){
                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();
                        String[] stopport = new String[1];
                        stopport[0] = m[1];
                        if(!(c_port.equals(m[1]))){
                            mContentResolver.delete(mUri, "@", stopport);
                        }
                    }
                    if(m[0].equals("del_specific")){
                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        mContentResolver.delete(mUri,m[1], null);
                    }

                    if(m[0].equals("hop")){

                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();

                        Log.d("Server","inside hop");
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD,m[1]);
                        cv.put(VALUE_FIELD,m[2]);
                        Log.d("Insert=method",m[1].toString());
                        Log.d("insert2",m[2].toString());
                        Log.d("insert3",cv.toString());
                        Log.d("cv : ", mContentResolver.toString());
                        mContentResolver.insert(mUri, cv);
                    }
                    if(m[0].equals("Qhop")) {

                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();


                        Log.d("Server","inside Qhop");
                        String q_port = m[1];
                        String q_key = m[2];
                        String m_key_port = q_key+":"+q_port;
                        mContentResolver.query(mUri, null, m_key_port, null, null);
                    }
                    if(m[0].equals("rhop")) {

                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();

                        Log.d("Server","inside rhop");
                        String r_key = m[1];
                        String r_val = m[2];
                        Log.d("Serverhop:",m[1]+" "+m[2]);
                        String res = m[1]+"*"+m[2];
                        queue.put(res);

                    }
                    if(m[0].equals("GDQuery")){

                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();


                        Log.d("Server","inside GDQuery");
                        String[] q_port = new String[2];
                        q_port[0] = m[1];
                        q_port[1] = "first";
                        Log.d("GDQuery:",m[1]);
                        mContentResolver.query(mUri,null,"*",q_port,null );

                    }
                    if(m[0].equals("GDRQuery")){

                        dout = new DataOutputStream(sock.getOutputStream());
                        ack="ack";
                        ack.trim();
                        dout.writeUTF(ack);
                        dout.flush();

                        Log.d("Server","inside GDRQuery");
                        String[] port_msg = new String[2];
                        port_msg[0] = m[1];
                        port_msg[1] = m[2];
                        Log.d("GDQuery:",m[1]+ " "+m[2]);
                        if(c_port.equals(port_msg[0])){
                            Log.d("GDQuery","Same port! populate blocking queue!");
                            squeue.put(port_msg[1]);
                        }
                        else
                        {
                            mContentResolver.query(mUri,null,"*",port_msg,null );
                        }
                    }

                     sock.close();
                     Log.d("Server", "Message complete");
                 }catch (Exception e) {
                     Log.e(TAG, "No Connection");
                     e.printStackTrace();
                 }
             }

         }

         protected void onProgressUpdate(String... strings) {
         }
     }

    public void display_nodes(Node node){
        Log.d("Port Number",node.port_num);
        Log.d("Hash Value",node.hash_port);
        if(node.succesor == null)
            Log.d("Successor Node id", "null");
        else Log.d("Successor Node id", node.succesor.port_num);
        if(node.predecessor == null)
            Log.d("Predecessor Node id", "null");
        else Log.d("Predecessor Node id", node.predecessor.port_num);
     }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

        Log.d("Client","Message recieved is:"+msgs[0]);

        String[] pstr = msgs[0].split("\\:");

        if(pstr[0].equals("Join")) {

            Log.d("FinalClient","Join: "+pstr[1]);

            try {
                String add_node = pstr[1]; //5556 or 5558 etc
                //Pinging 5554
                String remotePort = avd_id.get("5554");

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = "add:" + add_node; //5556
                msgToSend = msgToSend.trim();
                Log.d("FinalClient", "Message to be sent:" + msgToSend);

                DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                dout.writeUTF(msgToSend);
                dout.flush();


                DataInputStream din = new DataInputStream(socket.getInputStream());
                String message = din.readUTF();
                String[] list = message.split("\\:");

                socket.close();

                Log.d("FinalClient","Message received"+ message.toString() );

//                for(int i = 0; i < list.length;i++){
//                    Log.d("list:",list[i]);
//                }
                if (list[0].equals("list")) {

                    Log.d("ListLength",": "+list.length);
                    //socket.close();
                    //add them to your list
                    for(int i = 1; i < list.length;i++) {

                        String port = list[i];
                        String hash = genHash(port);
                        Node no = new Node(port, hash);
                        Log.d("Step1: Node:" + port + "Size", ": " + nodes.size());

                        if (!(set.contains(port))) {
                            set.add(port);
                            nodes.add(no);
                        }
                        Log.d("Step2: Node:" + port + "Size", ": " + nodes.size());

                        //display nodes before adding succ n preds
//                         for(i = 0; i < nodes.size();i++) {
//                            display_nodes(nodes.get(i));
//                         }
                    }

                    Collections.sort(nodes);
                    Log.d("Server", "Node size :" + nodes.size());
                         if(nodes.size() == 1)
                         {
                             Node new_nd = nodes.get(0);
                             new_nd.setSuccesor(new_nd);
                             new_nd.setPredecessor(new_nd);
                         }
                         else if(nodes.size() == 2 || nodes.size() == 3 || nodes.size() == 4 || nodes.size() == 5)
                         {
                             int n = nodes.size();
                             for(int i = 0; i < n; i++)
                             {
                                 if(i == 0) {
                                     nodes.get(i).setSuccesor(nodes.get(i + 1));
                                     nodes.get(i).setPredecessor(nodes.get(n - 1));

                                 }
                                 if(i == n-1){
                                     nodes.get(i).setSuccesor(nodes.get(0));
                                     nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if((i > 0) && (i <= (n - 2))){
                                   nodes.get(i).setSuccesor(nodes.get(i+1));
                                   nodes.get(i).setPredecessor(nodes.get(i-1));

                                 }
                                 if(nodes.get(i).port_num.equals(c_port)){
                                     c_pred = nodes.get(i).predecessor.port_num;
                                     c_succ = nodes.get(i).succesor.port_num;

                                     c_pred_hash = genHash(c_pred);
                                     c_succ_hash = genHash(c_succ);

                                 }
                                 Log.d("Closure:",":"+c_port+":"+c_hash+":"+c_succ+":"+c_pred);
                                 Log.d("Closure: Node Size:",":"+nodes.size());
                                 for(int k = 0; k < nodes.size();k++)
                                     display_nodes(nodes.get(k));{
                                    }
                             }
                         }

                         Log.d("Server","Sorted array :");
                         //display nodes after adding succ n preds
//                         for(i = 0; i < nodes.size();i++)
//                             display_nodes(nodes.get(i));{
//                        }

//
//                    for(int i = 0; i < list.length;i++){
//                        Log.d("list:",list[i]);
//                    }
                    //tell them to add you to their list except 5554
                     Log.d("No of Extra nodes",":"+(list.length-1));
                        for(int i = 1; i < list.length;i++){

                            String rPort = avd_id.get(list[i]);
                            Log.d("rport:",rPort);
                                if(rPort.equals("11108"))
                                    continue;

                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(rPort));
                                String msg = "broadcast:" + c_port;
                                msg = msg.trim();
                                Log.d("Client", "Message to be sent:" + msg);

                                dout = new DataOutputStream(socket.getOutputStream());
                                dout.writeUTF(msg);
                                dout.flush();
                                din = new DataInputStream(socket.getInputStream());
                                String ack = din.readUTF();

                                if (ack.equals("ack")) {
                                    Log.d("Client", "Insert Msg sent");
                                   socket.close();
                                }

                        }

                    }
//                    else if(message.equals(null) || message.isEmpty()){
//                    socket.close();
//
                }

            catch (Exception e){
                Log.e("Client", "ClientTask socket IOException");
            }


        }
        if(pstr[0].equals("succ")) {
            try {
                Log.d("Client", "Message recieved : " + msgs[0]);
                String succ_portno = pstr[1];
                String succ_porthash = pstr[2];
                String key = pstr[3];
                String val = pstr[4];
                Log.d("Client", "Hopping to successor :" + succ_portno);
                String remotePort = avd_id.get(succ_portno);
                Log.d("Creating a Socket with",":"+avd_id.get(succ_portno));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = "hop:" + key + ":" + val;
                msgToSend = msgToSend.trim();
                Log.d("Client", "Insert Message hopped to:" + msgToSend);
                DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                dout.writeUTF(msgToSend);
                dout.flush();
                DataInputStream din = new DataInputStream(socket.getInputStream());
                String ack = din.readUTF();
                if (ack.equals("ack")) {
                    Log.d("Client", "Insert Msg sent");
                }
                socket.close();


            } catch (Exception e) {
                Log.e("Client", "ClientTask socket IOException");
                e.printStackTrace();
            }
        }
        if(pstr[0].equals("qsucc")){

            try {
                    Log.d("Client", "Message recieved : " + msgs[0]);
                    String succ_port = pstr[1];
                    String q_port = pstr[2];
                    String q_key = pstr[3];
                    String q_hkey = pstr[4];
                    Log.d("Client", "Hopping to successor :" + succ_port);
                    String remotePort = avd_id.get(succ_port);
                    Log.d("Creating a Socket with",":"+avd_id.get(succ_port));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String msgToSend = "Qhop:" + q_port + ":" + q_key +":"+ q_hkey;
                    msgToSend = msgToSend.trim();
                    Log.d("Client", "Query Message hopped to:" + msgToSend);

                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();

                    if (ack.equals("ack")) {
                        Log.d("Client", "Query Msg sent");
                        socket.close();
                        }

            }
                catch (Exception e) {
                Log.e("Client", "ClientTask socket IOException");
                e.printStackTrace();
                }
        }


            if(pstr[0].equals("res")){
            try{
                Log.d("Client", "Message recieved : " + msgs[0]);
                String origin  = pstr[1];
                String key = pstr[2];
                String value = pstr[3];

                Log.d("Client", "Sending result to the original avd :" + origin);
                    String remotePort = avd_id.get(origin);

                    Log.d("Creating a Socket with",":"+avd_id.get(origin));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String msgToSend = "rhop:" + key +":"+ value;
                    msgToSend = msgToSend.trim();
                    Log.d("Client", "Result Message hopped is:" + msgToSend);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if (ack.equals("ack")) {
                        Log.d("Client", "QueryResult Msg sent");
                        socket.close();
                    }

                    }
            catch (Exception e){
                Log.e("Client", "ClientTask socket IOException");
                e.printStackTrace();
            }


            }
            if(pstr[0].equals("@query")) {
                try {
                    Log.d("Client", "Message recieved : " + msgs[0]);
                    String succ = pstr[1];
                    String ac_port = pstr[2];
                    Log.d("pstr[3]",pstr[3]);
                    if(!(pstr[3].equals("first")))
                    {
                        Log.d("Client", "Inside GDRQuery");
                        Log.d("Client", "Sending result to the succ avd :" + succ);
                    String remotePort = avd_id.get(succ);
                    Log.d("Creating a Socket with", ":" + avd_id.get(succ));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String msgToSend = "GDRQuery:"+ ac_port + ":" + pstr[3];
                    msgToSend = msgToSend.trim();
                    Log.d("Client", "Result Message hopped is:" + msgToSend);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();
                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if (ack.equals("ack")) {
                        Log.d("Client", "QueryResult Msg sent");
                        socket.close();
                    }

                    }
                    else {
                        Log.d("Client","inside gdquery");
                        Log.d("Client", "Sending result to the succ avd :" + succ);
                        String remotePort = avd_id.get(succ);
                        Log.d("Creating a Socket with", ":" + avd_id.get(succ));
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        String msgToSend = "GDQuery:" + ac_port;
                        msgToSend = msgToSend.trim();
                        Log.d("Client", "Result Message hopped is:" + msgToSend);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        dout.writeUTF(msgToSend);
                        dout.flush();
                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack = din.readUTF();
                        if (ack.equals("ack")) {
                            Log.d("Client", "QueryResult Msg sent");
                            socket.close();
                        }

                        }

                }catch (Exception e){
                    Log.e("Client", "ClientTask socket IOException");
                    e.printStackTrace();
                }
            }
            if(pstr[0].equals("*del*")){
                try {
                    String scp = pstr[1];
                    String cu_p = pstr[2];
                    Log.d("Client", "inside *del*");
                    Log.d("Client", "Calling succ avd :" + scp);
                    String remotePort = avd_id.get(scp);
                    Log.d("Creating a Socket with", ":" + avd_id.get(scp));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String msgToSend = "del:" + cu_p;
                    msgToSend = msgToSend.trim();
                    Log.d("Client", "delete Message hopped is:" + msgToSend);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();
                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if (ack.equals("ack")) {
                        Log.d("Client", "Delete Msg sent");
                        socket.close();
                    }
                    }catch (Exception e){
                    Log.d("Client", "Delete Msg not sent");
                    }
            }
            if(pstr[0].equals("*del_specific*")){
                try{
                    String scp = pstr[1];
                    String sel = pstr[2];
                    Log.d("Client", "inside *del*");
                    Log.d("Client", "Calling succ avd :" + scp);
                    String remotePort = avd_id.get(scp);
                    Log.d("Creating a Socket with", ":" + avd_id.get(scp));
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String msgToSend = "del_specific:" + sel;
                    msgToSend = msgToSend.trim();
                    Log.d("Client", "delete Message hopped is:" + msgToSend);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();
                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if (ack.equals("ack")) {
                        Log.d("Client", "Delete Msg sent");
                        socket.close();
                    }
                }catch (Exception e){

                }
            }


            return null;
        }

 }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Log.d("Selection",selection);
        String result = "";
        String str_send = "";
        String final_s = "";
        String str_final = "";
        MatrixCursor cursor = null;
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        //Queried from some port other than c_port

        if(selection.contains(":"))
        {
            Log.d("Query","Queried from a different port!");
            String[] sel = selection.split("\\:");

            selection = sel[0];
            String check_port = sel[1];
             // TODO Auto-generated method stub
            try {
                h_key = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.d("Query", "Selection_Value"+selection);
            Log.d("Query","Hash to be queried: "+h_key);
            Log.d("Query", "Node Size :" + nodes.size());
            //non-edge case
            Log.d("Query:", "current : " + c_port + " hash : " + c_hash);
            Log.d("Query:", "predecessor : " + c_pred + " hash : " + c_pred_hash);

            if (c_hash.compareTo(c_pred_hash) > 0) {
                Log.d("Query", "Entering NonEdge Case");
                Log.d("Query : ","h_key"+h_key);
                if ((c_hash.compareTo(h_key) > 0) && (c_pred_hash.compareTo(h_key) < 0)) {
                    //belongs to curr node
                    Log.d("Query", "Belongs to current node!Performs query!");
                        try {
                            fis = getContext().openFileInput(selection);
                            isr = new InputStreamReader(fis);
                            br = new BufferedReader(isr);
                            result = br.readLine();
                            String imsg = String.format("res:%s:%s:%s:%s",check_port, selection, result , h_key);

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                            cursor = null;

                            br.close();
                            isr.close();
                            fis.close();
                        } catch (Exception e) {
                            Log.e("query", "Reading from file failed");
                        }
                    }
                    //doesnt belong to your node. route!!
                    else {
                        Log.d("query", "Routing!!!!!!!!!!!!!!!!!!!" + c_succ);
                        String imsg = String.format("qsucc:%s:%s:%s:%s", c_succ, check_port, selection, h_key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                        cursor = null;
                    }
                }
                //edge case
                else {
                    Log.d("query", "Entering Edge Case");
                    //belongs to curr node
                    if ((c_hash.compareTo(h_key) > 0) || (c_pred_hash.compareTo(h_key) < 0)) {
                        Log.d("query", "Belongs to current node!Performs querying!");
                        try {
                            fis = getContext().openFileInput(selection);
                            isr = new InputStreamReader(fis);
                            br = new BufferedReader(isr);
                            result = br.readLine();
                            String imsg = String.format("res:%s:%s:%s:%s",check_port, selection, result , h_key);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                            cursor = null;
                            br.close();
                            isr.close();
                            fis.close();
                        } catch (Exception e) {
                            Log.e("query", "Reading from file failed");
                        }
                    }
                    //doesnt belong to curr node, route!
                    else {
                        Log.d("query", "Routing!!!!!!!!!!!!!!!!!!!" + c_succ);
                        String imsg = String.format("qsucc:%s:%s:%s:%s", c_succ, check_port, selection, h_key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                        cursor = null;
                    }
                }

        }

        else if(selection.equals("@")){
        try {
            File dir = getContext().getFilesDir();
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if(files != null){
                cursor = new MatrixCursor(new String[]{"key", "value"});
                Log.d("**Query**","Total number of files : "+files.length);
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    String strFileName = file.getName();
                    Log.d("**Query**","File : "+strFileName);
                    fis = getContext().openFileInput(strFileName);
                    isr = new InputStreamReader(fis);
                    br = new BufferedReader(isr);
                    result = br.readLine();
                    Log.d("**query**","value:"+result);
                    cursor.addRow(new String[]{strFileName, result});
                    Log.d("**Query**","Total number of rows : "+files.length);
                    if(i == files.length - 1){
                        if(cursor.getCount() == files.length){
                            Log.d("**Query**","Success");
                        }
                    }
                }
            }
            }
        }catch (IOException e)
        {
        Log.d("**Query**","Caught IOException");
        }
        }
        else if(selection.equals("*")) {
            Log.d("GDump","inside *");
            if (selectionArgs == null) {

                if(nodes.size() == 1){
                     try {
                            File dir = getContext().getFilesDir();
                            File[] files = dir.listFiles();
                            if(files != null) {
                                Log.d("**GDump**", "Total number of files : " + files.length);
                                for (int i = 0; i < files.length; i++) {
                                    File file = files[i];
                                    String strFileName = file.getName();
                                    Log.d("**GDump**", strFileName);
                                    fis = getContext().openFileInput(strFileName);
                                    isr = new InputStreamReader(fis);
                                    br = new BufferedReader(isr);
                                    result = br.readLine();
                                    Log.d("**GDump**", result);
                                    String add_str = strFileName + "*" + result + "|";
                                    Log.d("**GDump**", add_str);
                                    sbuf.append(add_str);

                                }
                                String strf = sbuf.toString();
                                Log.d("final", sbuf.toString());
                                //seperate and return cursor
                                String[] pair = strf.split("\\|");
                                Log.d("pair length", ":" + pair.length);
                                for (int i = 0; i < pair.length; i++) {
                                    Log.d("=", pair[i]);
                                }
                                cursor = new MatrixCursor(new String[]{"key", "value"});
                                for (int i = 0; i < pair.length; i++) {
                                    Log.d("pair", pair[i]);
                                    String[] key_val = pair[i].split("\\*");
                                    String key = key_val[0];
                                    String val = key_val[1];
                                    Log.d("pairval:", key + " " + val);
                                    cursor.addRow(new String[]{key, val});
                                    Log.d("row", "added" + i);
                                }
                                Log.d("cursor rows:", "" + cursor.getCount());
                            }
                            else{
                                cursor = null;
                            }
                }catch (Exception e){
                         Log.d("**Query**", "Caught IOException");
                     }
                }


            else {

                    Log.d("Query*", "Node Size" + nodes.size());


                    Log.d("GDump", "Querying for Original port" + c_port);
                    try {
                        File dir = getContext().getFilesDir();
                        File[] files = dir.listFiles();
                        Log.d("**GDump**", "Total number of files : " + files.length);
                        if(files != null) {
                            for (int i = 0; i < files.length; i++) {
                                File file = files[i];
                                String strFileName = file.getName();
                                Log.d("**GDump**", "File : " + strFileName);
                                fis = getContext().openFileInput(strFileName);
                                isr = new InputStreamReader(fis);
                                br = new BufferedReader(isr);
                                result = br.readLine();
                                Log.d("**GDump**", ":" + result);
                                String add_str = strFileName + "*" + result + "|";
                                Log.d("GDump", add_str);
                                sbuf.append(add_str);
                            }
                            String imsg = String.format("@query:%s:%s:%s", c_succ, c_port, "first");
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                            squeue = new ArrayBlockingQueue<String>(1);
                            final_s = squeue.take();
                            Log.d("query:", "Blocking queue populated with:" + final_s);
                            str_final = sbuf.toString() + final_s;
                            Log.d("final", str_final);
                            String[] pair = str_final.split("\\|");
                            Log.d("pair lenght", "" + pair.length);
                            for (int i = 0; i < pair.length; i++) {
                                Log.d("=", pair[i]);
                            }
                            cursor = new MatrixCursor(new String[]{"key", "value"});
                            for (int i = 0; i < pair.length; i++) {
                                Log.d("pair", pair[i]);
                                String[] key_val = pair[i].split("\\*");
                                String key = key_val[0];
                                String val = key_val[1];
                                Log.d("pairval:", key + " " + val);
                                cursor.addRow(new String[]{key, val});
                                Log.d("row", "added" + i);
                            }
                            Log.d("cursor rows:", "" + cursor.getCount());
                        }
                        else{
                            cursor = null;
                        }
                    } catch (IOException e) {
                        Log.d("**Query**", "Caught IOException");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (!(selectionArgs == null)) {
            String q_port = selectionArgs[0];
            String q_msg = selectionArgs[1];//d)
                 Log.d("sel:",q_port+" "+q_msg);
                if(q_msg.equals("first"))
                {
                    q_msg = "";
                }
                try {
                    File dir = getContext().getFilesDir();
                        File[] files = dir.listFiles();
                        if (files != null) {
                            Log.d("**GDump**", "Total number of files : " + files.length);
                            for (int i = 0; i < files.length; i++) {
                                File file = files[i];
                                String strFileName = file.getName();
                                Log.d("**GDump**", "File : " + strFileName);
                                fis = getContext().openFileInput(strFileName);
                                isr = new InputStreamReader(fis);
                                br = new BufferedReader(isr);
                                result = br.readLine();
                                Log.d("**GDump**", ":" + result);
                                String add_str2 = strFileName + "*" + result + "|";
                                str_send += add_str2;
                            }

                            q_msg += str_send;
                            if (q_msg.equals("")) {
                                q_msg = "first";
                            }
                            String imsg = String.format("@query:%s:%s:%s", c_succ, q_port, q_msg);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                            cursor = null;
                        }
                        else{
                            cursor = null;
                        }
                }catch (IOException E){
                    Log.d("**Query**", "Caught IOException");
                }
            }
        }
        //Queried from your own port
        //returns cursor after blocking queue is populated
        else {
            Log.d("Query","Queried from own port!");
            // TODO Auto-generated method stub
            try {
                h_key = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            Log.d("Query", "Selection_Value"+selection);
            Log.d("Query","Hash to be queried: "+h_key);
            Log.d("Query", "Node Size :" + nodes.size());
            if (nodes.size() == 1) {
                Log.d("query", "Node Size : 1");
                try {
                    fis = getContext().openFileInput(selection);
                    isr = new InputStreamReader(fis);
                    br = new BufferedReader(isr);
                    result = br.readLine();
                    cursor = new MatrixCursor(new String[]{"key", "value"});
                    cursor.addRow(new String[]{selection, result});
                    br.close();
                    isr.close();
                    fis.close();
                } catch (Exception e) {
                    Log.e("query", "Reading from file failed");
                }
            } else if (nodes.size() > 1) {
                Log.d("query", "Node Size :" + nodes.size());
                //non-edge case
                Log.d("query:", "current:" + c_port + "hash" + c_hash);
                Log.d("query:", "current:" + c_pred + "hash" + c_pred_hash);
                if (c_hash.compareTo(c_pred_hash) > 0) {
                    Log.d("query", "Entering NonEdge Case");
                    if ((c_hash.compareTo(h_key) > 0) && (c_pred_hash.compareTo(h_key) < 0)) {
                        //belongs to curr node
                        Log.d("query", "Belongs to current node!Performs query!");
                        try {
                            fis = getContext().openFileInput(selection);
                            isr = new InputStreamReader(fis);
                            br = new BufferedReader(isr);
                            result = br.readLine();
                            cursor = new MatrixCursor(new String[]{"key", "value"});
                            cursor.addRow(new String[]{selection, result});
                            br.close();
                            isr.close();
                            fis.close();
                        } catch (Exception e) {
                            Log.e("query", "Reading from file failed");
                        }
                    }
                    //doesnt belong to your node. route!!
                    else {
                        Log.d("query", "Routing!!!!!!!!!!!!!!!!!!!" + c_succ);
                        String imsg = String.format("qsucc:%s:%s:%s:%s", c_succ, c_port, selection, h_key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                        try {
                        queue = new ArrayBlockingQueue<String>(1);
                        String res = queue.take();
                        String[] r = res.split("\\*");
                        cursor = new MatrixCursor(new String[]{"key", "value"});
                        cursor.addRow(new String[]{r[0], r[1]});
                        }catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                    }
                }
                //edge case
                else {
                    Log.d("query", "Entering Edge Case");
                    //belongs to curr node
                    if ((c_hash.compareTo(h_key) > 0) || (c_pred_hash.compareTo(h_key) < 0)) {
                        Log.d("query", "Belongs to current node!Performs querying!");
                        try {
                            fis = getContext().openFileInput(selection);
                            isr = new InputStreamReader(fis);
                            br = new BufferedReader(isr);
                            result = br.readLine();
                            cursor = new MatrixCursor(new String[]{"key", "value"});
                            cursor.addRow(new String[]{selection, result});
                            br.close();
                            isr.close();
                            fis.close();
                        } catch (Exception e) {
                            Log.e("query", "Reading from file failed");
                        }
                    }
                    //doesnt belong to curr node, route!
                    else {
                        Log.d("query", "Routing!!!!!!!!!!!!!!!!!!!" + c_succ);
                        String imsg = String.format("qsucc:%s:%s:%s:%s", c_succ, c_port, selection, h_key);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, imsg, myPort);
                        try {
                            queue = new ArrayBlockingQueue<String>(1);
                            String res = queue.take();
                            String[] r = res.split("\\*");
                            cursor = new MatrixCursor(new String[]{"key", "value"});
                            cursor.addRow(new String[]{r[0], r[1]});
                        }catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        return cursor;

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


}

