package edu.buffalo.cse.cse486586.simpledynamo;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import android.database.Cursor;
import android.net.Uri;
import android.content.Context;
import android.database.MatrixCursor;
import java.net.ServerSocket;
import android.os.SystemClock;
import java.net.InetAddress;
import java.net.Socket;
import android.util.Log;
import java.io.Serializable;
import java.io.IOException;
import android.telephony.TelephonyManager;
import java.io.ObjectInputStream;
import java.util.Date;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.HashMap;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.os.AsyncTask;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;



public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int FIRST_NODE = 11108;
    //static final String[] REMOTE_PORT_ARRAY = {"11108","11112","11116","11120","11124"};
    static Hashtable<String, String> MyDB = new Hashtable<String, String>();

    final int TIMEOUT = 2000;

    static Map<String, String> resultOfMyQuery = new HashMap<String, String>();
    static int MyPortNumber = 0;
    final String HISTORY = "EXISTS";


    static List<Integer> REMOTE_PORT_ARRAY = new ArrayList<Integer>(Arrays.asList(5562,5556,5554,5558,5560));
    //static List<Integer> REMOTE_PORT_ARRAY = new ArrayList<Integer>(Arrays.asList(5554,5556,5558,5560,5562));
    static Map<Integer, String> HASHED_REMOTE_PORT_ARRAY = new HashMap<Integer, String>();

    static boolean recoveryPhase;
    static int recoveryAidsReceived;

    String INSERT_REQUEST = "INSERT_REQUEST";
    String PERFORM_INSERTION = "PERFORM_INSERTION";
    String QUERY_FIND_REQUEST = "QUERY_FIND_REQUEST";
    String RETURN_QUERY_RESULT = "RETURN_QUERY_RESULT";
    String STAR_QUERY_REQ = "STAR_QUERY_REQ";
    String EXECUTE_STAR = "EXECUTE_STAR";
    String DELETE_STAR_REQUEST = "DELETE_STAR_REQUEST";
    String DELETE_SINGLE_KEY_REQUEST = "DELETE_SINGLE_KEY_REQUEST";
    String INSERT_ACK = "INSERT_ACK";
    String RECOVERY_DATA_REQ = "RECOVERY_DATA_REQ";
    String RECEIVE_RECOVERY_DATA = "RECEIVE_RECOVERY_DATA";
    String PERFORM_RECOVERY = "PERFORM_RECOVERY";
    String Delimiter = "##";
    String Delimiter2 = "###";
    String KeyValueSeparator = "=>";
    Boolean insertack=false;
    Boolean queryack=false;


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        if (recoveryPhase) {
            while (recoveryPhase) ;
        }
        int ownerKey = owner(values.get("key").toString());
        List<Integer> backups = backupsincludingMyself(ownerKey);

        for (int Idx : backups) {
            long Starttime = System.currentTimeMillis();
            insertack=false;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_REQUEST, values.get("key").toString(), values.get("value").toString(), String.valueOf(Idx));
            while(!insertack)
            {
                long currenttime = System.currentTimeMillis();
                if((currenttime - Starttime) > TIMEOUT)
                {
                    break;
                }
            }
        }

        return null;
    }


    @Override
    public int delete(Uri uri, String msgKey, String[] selectionArgs) {

        if (recoveryPhase) {
            while (recoveryPhase) ;
        }

        if (msgKey.contains("*")) {
            for (int portNum : REMOTE_PORT_ARRAY)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_STAR_REQUEST, String.valueOf(portNum));

        } else if (msgKey.contains("@")) {
            for (String key : getContext().fileList())
                if (key.equals(HISTORY)==false)
                    DeleteFile(key);
        } else {
            int ownerKey = owner(msgKey);
            List<Integer> backups = backupsincludingMyself(ownerKey);
            for (int Idx : backups)
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_SINGLE_KEY_REQUEST, msgKey, String.valueOf(Idx));
        }

        return 0;
    }


    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //MyPortNumber = Integer.parseInt(portStr) * 2;
        MyPortNumber = Integer.parseInt(portStr);

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return true;
        }


        for (int number : REMOTE_PORT_ARRAY) {
            try {
                HASHED_REMOTE_PORT_ARRAY.put(number, genHash(String.valueOf(number)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }


        recoveryPhase = recoveryPhase();
        recoveryAidsReceived = 0;
        if(recoveryPhase) {
            for (String key : getContext().fileList())
                if (key.equals(HISTORY) == false)
                    DeleteFile(key);
        }

        if (recoveryPhase) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RECOVERY_DATA_REQ);
        }
        return false;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String msgKey, String[] selectionArgs,
                        String sortOrder) {

        if (recoveryPhase) {
            while (recoveryPhase) ;
        }
        String[] columnNames = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);

        if (msgKey.contains("*")) {


            for (int portNum : REMOTE_PORT_ARRAY) {

                long Starttime = System.currentTimeMillis();
                queryack=false;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, STAR_QUERY_REQ, String.valueOf(portNum));

                while(!queryack)
                {
                    long currenttime = System.currentTimeMillis();
                    if((currenttime - Starttime) > TIMEOUT)
                    {
                        break;
                    }
                }

            }


            Map<String, String> resultsMap = new HashMap<String, String>();
            for (int portNum : REMOTE_PORT_ARRAY) {
                if (resultOfMyQuery.containsKey("*" + Delimiter2 + String.valueOf(portNum))) {
                    String keyValuePairsString = resultOfMyQuery.get("*" + Delimiter2 + String.valueOf(portNum));
                    ReciveDataFromReplicas(keyValuePairsString, resultsMap);
                }
            }

            parsedatafromReplicas(resultsMap, matrixCursor);

        } else if (msgKey.contains("@")) {

            for (String key : getContext().fileList()) {
                if (key.equals(HISTORY) == false) {
                    reurnValueBackToScript(key, matrixCursor);
                }
            }

        } else {

            int ownerKey = owner(msgKey);
            List<Integer> backups = backupsincludingMyself(ownerKey);

            for (int Idx : backups) {
                long Starttime = System.currentTimeMillis();
                queryack=false;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY_FIND_REQUEST, msgKey, String.valueOf(Idx));

                while(!queryack)
                {
                    long currenttime = System.currentTimeMillis();
                    if((currenttime - Starttime) > TIMEOUT)
                    {
                        break;
                    }
                }
            }


            List<String> resultList = new ArrayList<String>();
            for (int Idx : backups) {
                if (resultOfMyQuery.containsKey(msgKey + Delimiter2 + String.valueOf(Idx))) {
                    resultList.add(resultOfMyQuery.get(msgKey + Delimiter2 + String.valueOf(Idx)));
                }
            }

            String latest = "";
            long latestVersion = 0;
            for (String result : resultList) {
                if (result != null && !result.trim().isEmpty()) {
                    long thisVersion = ExtractVersion(result);
                    if (thisVersion > latestVersion) {
                        latestVersion = thisVersion;
                        latest = result;
                    }
                }
            }
            String[] columnValues = {msgKey, giveMeValue(latest)};
            matrixCursor.addRow(columnValues);
        }

        return matrixCursor;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Socket Socket;
            ServerSocket serverSocket = sockets[0];


            try {
                while (true) {
                    Socket = serverSocket.accept();


                    BufferedReader br = new BufferedReader(new InputStreamReader(Socket.getInputStream()));
                    String received = br.readLine();

                    if (received != null) {
                        String received_Array[] = received.split(Delimiter);
                        String CommandType = received_Array[0];

                        if (CommandType.equals(INSERT_REQUEST)) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, PERFORM_INSERTION, received_Array[1], received_Array[2], received_Array[3]);
                        } else if (CommandType.equals(QUERY_FIND_REQUEST)) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RETURN_QUERY_RESULT, received_Array[1], received_Array[2]);
                        } else if (CommandType.equals(RETURN_QUERY_RESULT)) {
                            resultOfMyQuery.put(received_Array[1] + Delimiter2 + received_Array[3], received_Array[2]);
                            queryack=true;
                        } else if (CommandType.equals(INSERT_ACK)) {
                            insertack=true;
                        } else if (CommandType.equals(RECOVERY_DATA_REQ)) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(RECEIVE_RECOVERY_DATA), received_Array[1]);
                        } else if (CommandType.equals(RECEIVE_RECOVERY_DATA)) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(PERFORM_RECOVERY), received_Array[1], received_Array[2]);
                        } else if (CommandType.equals(STAR_QUERY_REQ)) {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, EXECUTE_STAR, received_Array[1]);
                        } else if (CommandType.equals(DELETE_SINGLE_KEY_REQUEST)) {
                            DeleteFile(received_Array[1]);
                        } else if (CommandType.equals(DELETE_STAR_REQUEST)) {

                            for (String key : getContext().fileList()) {
                                if (!key.equals(HISTORY)) {
                                    DeleteFile(key);
                                }
                            }

                        }

                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {

            String CommandType = (String) params[0];


            if (CommandType.equals(INSERT_REQUEST)) {
                String TxMsg = INSERT_REQUEST + Delimiter + params[1] + Delimiter + params[2] + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[3]) * 2);
            } else if (CommandType.equals(QUERY_FIND_REQUEST)) {
                String txMsg = QUERY_FIND_REQUEST + Delimiter + String.valueOf(MyPortNumber) + Delimiter + params[1];
                Transmit(txMsg, Integer.parseInt(params[2]) * 2);
            } else if (CommandType.equals(RETURN_QUERY_RESULT)) {
                String data = fetchDataFromDisk(params[2]);
                String TxMsg = RETURN_QUERY_RESULT + Delimiter + params[2] + Delimiter + data + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[1]) * 2);

            } else if (CommandType.equals(STAR_QUERY_REQ)) {
                String TxMsg = STAR_QUERY_REQ + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[1]) * 2);
            } else if (CommandType.equals(RECOVERY_DATA_REQ)) {
                String TxMsg = RECOVERY_DATA_REQ + Delimiter + String.valueOf(MyPortNumber);
                for (int x : REMOTE_PORT_ARRAY)
                    if (MyPortNumber != x)
                        Transmit(TxMsg, x * 2);
            }
            else if (CommandType.equals(RECEIVE_RECOVERY_DATA)) {

                int crashedid  = whatToReturn(Integer.parseInt(params[1]));
                String data = evrything(true, crashedid);
                String TxMsg = RECEIVE_RECOVERY_DATA + Delimiter + data + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[1]) * 2);
            }
            else if (CommandType.equals(DELETE_SINGLE_KEY_REQUEST)) {
                String TxMsg = DELETE_SINGLE_KEY_REQUEST + Delimiter + params[1] + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[2]) * 2);
            } else if (CommandType.equals(DELETE_STAR_REQUEST)) {
                String TxMsg = DELETE_STAR_REQUEST + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(params[1]) * 2);
            } else if (CommandType.equals(PERFORM_INSERTION)) {
                String key = params[1];
                String value = params[2];
                int originatorsPortId11 = Integer.parseInt(params[3]);

                long version = new Date().getTime();
                String currentData = fetchDataFromDisk(key);
                flushToDisk(key, String.valueOf(version) + "@@@" + value);
                String TxMsg = INSERT_ACK + Delimiter + key + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, originatorsPortId11 * 2);
            } else if (CommandType.equals(PERFORM_RECOVERY)) {
                String data = params[1];
                Map<String, String> temp = new HashMap<String, String>();
                ReciveDataFromReplicas(data, temp, true);

                for (String key : temp.keySet()) {
                    String actualValue = temp.get(key);
                    flushToDisk(key, actualValue);
                }

                recoveryAidsReceived++;
                if (recoveryAidsReceived == 4) {
                    recoveryPhase = false;
                }

            } else if (CommandType.equals(EXECUTE_STAR)) {
                String source = params[1];
                String data = evrything();


                String TxMsg = RETURN_QUERY_RESULT + Delimiter + "*" + Delimiter + data + Delimiter + String.valueOf(MyPortNumber);
                Transmit(TxMsg, Integer.parseInt(source) * 2);
            }

            return null;
        }

    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        //String x = String.valueOf(MyNextNodePortNumber);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private void flushToDisk(String fileName, String contentOfFile) {
        try {
            FileOutputStream stream = getContext().openFileOutput(fileName, Context.MODE_WORLD_WRITEABLE);
            stream.write(contentOfFile.getBytes());
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String fetchDataFromDisk(String fileName) {
        String contentOfFile = "";
        try {
            File file = getContext().getFileStreamPath(fileName);
            if (file.exists()) {
                FileInputStream stream = getContext().openFileInput(fileName);
                int byteContent;
                if (stream != null) {
                    while ((byteContent = stream.read()) != -1)
                        contentOfFile += (char) byteContent;
                    stream.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentOfFile;
    }

    private int owner(String msgKey) {
        try {
            String hashedKey = genHash(msgKey);
            for (int portId : REMOTE_PORT_ARRAY)
                if (doesItBelongHere(hashedKey, portId))
                    return portId;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return -1;
    }

    int my_idx = -1;

    private Integer whatToReturn(Integer idOfRecoveringDevice){
        for(int idx = 0;idx<5;idx++)

        {
            if (REMOTE_PORT_ARRAY.get(idx) == MyPortNumber) {
                my_idx = idx;
                break;
            }
        }

        //check if this node is my successor. I need to send My data
        for(
                int i = 1;
                i<3;i++)

        {

            if (REMOTE_PORT_ARRAY.get((my_idx + i) % 5) == idOfRecoveringDevice)

            {
                return MyPortNumber;
            }
        }


        //THis node is behind Me. I was supposed to store his data. Hence check for His data
        return idOfRecoveringDevice;
    }


    private String giveMeValue(String content) {
        return content.substring(content.indexOf("@@@") + "@@@".length());
    }

    private boolean doesItBelongHere(String msgKeyHashed, int nodeId) {

        String myHashedId = HASHED_REMOTE_PORT_ARRAY.get(nodeId);
        boolean IamOwner = false;



        int myidx = -1;
        for (int i = 0; i < 5; i++) {
            if (REMOTE_PORT_ARRAY.get(i) == nodeId)
                myidx = i;
        }

        int predecessorId = 0;


        if(myidx > 0)
        {
            predecessorId = REMOTE_PORT_ARRAY.get(myidx - 1);

        }
        else
        {
            predecessorId = REMOTE_PORT_ARRAY.get(4);
        }




        String predecessorHashedId = HASHED_REMOTE_PORT_ARRAY.get(predecessorId);
        int successorId = REMOTE_PORT_ARRAY.get((myidx + 1) % 5);
        String successorHashedId = HASHED_REMOTE_PORT_ARRAY.get(successorId);


        if(IsThereOnlyOneNodeInRing(myHashedId,predecessorHashedId,successorHashedId))
        {
            IamOwner=true;
        }
        else if (CornorCase_IfBetweenFirstAndLastOfRing(myHashedId,predecessorHashedId,msgKeyHashed))
        {
            IamOwner=true;
        }

        else if(InsertKeyAtCorrectLocation(myHashedId,predecessorHashedId,msgKeyHashed))
        {
            IamOwner=true;
        }

        return IamOwner;
    }


    private boolean IfKeyAndNodeMatch(String key, int recoveringDeviceId) {
        int coordinatorId = owner(key);
        List<Integer> backups = backupsincludingMyself(coordinatorId);

        return backups.contains(recoveringDeviceId);
    }

    public boolean IsThereOnlyOneNodeInRing(String MyNodeHashID, String MyPreviousNodeHashID, String successorHashedId) {

        if ((MyNodeHashID.compareTo(MyPreviousNodeHashID)==0) && (MyNodeHashID.compareTo(successorHashedId)==0))
        {
            return true;
        }
        return false;
    }


    public boolean CornorCase_IfBetweenFirstAndLastOfRing(String MyNodeHashID, String MyPreviousNodeHashID, String key) {

        if ((MyPreviousNodeHashID.compareTo(MyNodeHashID) > 0) && (key.compareTo(MyPreviousNodeHashID) < 0 && key.compareTo(MyNodeHashID) <= 0))
        {
            return true;
        }
        if ((MyPreviousNodeHashID.compareTo(MyNodeHashID) > 0) && (key.compareTo(MyPreviousNodeHashID) > 0 && key.compareTo(MyNodeHashID) > 0))
        {
            return true;
        }
        return false;
    }


    public boolean InsertKeyAtCorrectLocation(String MyNodeHashID, String MyPreviousNodeHashID, String key)
    {

        if (key.compareTo(MyPreviousNodeHashID) > 0 && key.compareTo(MyNodeHashID) < 1)
        {
            return true;
        }
        return false;
    }


    private long ExtractVersion(String value) {
        return Long.parseLong(value.substring(0, value.indexOf("@@@")));
    }



    private static List<Integer> backupsincludingMyself(int node) {
        List<Integer> backup = new ArrayList<Integer>();
        int idx = -1;
        for (int i = 0; i < REMOTE_PORT_ARRAY.size(); i++)
            if (REMOTE_PORT_ARRAY.get(i) == node) {
                idx = i;
                break;
            }
        backup.add(REMOTE_PORT_ARRAY.get(idx));
        backup.add(REMOTE_PORT_ARRAY.get((idx + 1) % (5)));
        backup.add(REMOTE_PORT_ARRAY.get((idx + 2) % (5)));
        return backup;
    }

    public boolean recoveryPhase() {
        boolean recoveryPhase = false;

        File file = getContext().getFileStreamPath(HISTORY);
        if (file.exists())
            recoveryPhase = true;
        else
            flushToDisk(HISTORY, HISTORY);

        return recoveryPhase;
    }

    private void reurnValueBackToScript(String key, MatrixCursor matrixCursor) {
        String data = fetchDataFromDisk(key);
        String fileContent = giveMeValue(data);

        if (fileContent != null && fileContent.length() > 0) {
            String[] columnValues = new String[2];
            columnValues[0] = key;
            columnValues[1] = fileContent;
            matrixCursor.addRow(columnValues);
        }
    }

    private void Transmit(String messageToBeSent, int destinationPort) {
        Socket socket;
        try {

            socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    destinationPort));

            OutputStream x = socket.getOutputStream();
            BufferedWriter send_data = new BufferedWriter(new OutputStreamWriter(x));
            StringBuffer sendString = new StringBuffer();
            send_data.write(messageToBeSent);
            send_data.flush();
            send_data.close();
            socket.close();



        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public String evrything() {
        return evrything(false, -1);
    }

    public void parsedatafromReplicas(Map<String, String> temp, MatrixCursor matrixCursor) {
        for (String key: temp.keySet()) {
            String actualValue = giveMeValue(temp.get(key));
            String[] columnValues = {key, actualValue};
            matrixCursor.addRow(columnValues);
        }
    }

    public void ReciveDataFromReplicas(String data, Map<String, String> temp) {
        ReciveDataFromReplicas(data, temp, false);
    }

    public String evrything(boolean recoveryMode, int id) {
        String allKeyValuePairs = "";
        for (String key : getContext().fileList()) {
            if (!key.equals(HISTORY)) {
                boolean valid = true;
                if (recoveryMode && !IfKeyAndNodeMatch(key, id)) {
                    valid = false;
                }

                if (valid) {
                    allKeyValuePairs += key + KeyValueSeparator + fetchDataFromDisk(key) + ",";
                }
            }
        }

        if (!allKeyValuePairs.isEmpty() && allKeyValuePairs.charAt(allKeyValuePairs.length() - 1) == ',')
            allKeyValuePairs = allKeyValuePairs.substring(0, allKeyValuePairs.length() - 1);

        return allKeyValuePairs;
    }

    public void ReciveDataFromReplicas(String aggregatedString, Map<String, String> resultsMap, boolean operating) {

        if (aggregatedString.contains(",")) {
            String[] splitted = aggregatedString.trim().split(",");
            for (String kvPair : splitted) {

                if (kvPair.contains(KeyValueSeparator)) {
                    String[] keyValue = kvPair.split(KeyValueSeparator);
                    String key = keyValue[0];
                    String value = keyValue[1];
                    long current = ExtractVersion(value);

                    if (operating) {
                        String x = fetchDataFromDisk(key);
                        if (!x.isEmpty()) {
                            long best = ExtractVersion(x);

                            if (current < best) {
                                value = x;
                            }
                        }
                    }

                    if ((operating==false) && resultsMap.containsKey(key)) {
                        long existingVersion = ExtractVersion(resultsMap.get(key));

                        if (current < existingVersion)
                            value = resultsMap.get(key);
                    }

                    resultsMap.put(key, value);
                }
            }
        }
    }



    private void DeleteFile(String key) {
        File file = getContext().getFileStreamPath(key);
        if (file.exists()) {
            file.delete();
        }
    }


}