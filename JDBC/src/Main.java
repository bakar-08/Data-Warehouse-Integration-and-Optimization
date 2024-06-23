import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import java.util.Collection;
import java.util.Map;


public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter MySQL database URL:");
        String url = scanner.nextLine();

        System.out.println("Enter MySQL username:");
        String username = scanner.nextLine();

        System.out.println("Enter MySQL password:");
        String password = scanner.nextLine();

        /*
        url = "jdbc:mysql://localhost:3306/ELECTRONICA-DW";
        username = "root";
        password = "HelloWorld123!";
        */
        MultiHashMap transMap = new MultiHashMap();

        StreamGeneratorThread streamer = new StreamGeneratorThread(url, username, password, transMap);
        HybridJoinThread hybridJoin = new HybridJoinThread(url, username, password, transMap);
        ControllerThread controller = new ControllerThread(streamer, hybridJoin);

        streamer.start();
        //hybridJoin.start();
        //controller.start();


    }
}

    class StreamGeneratorThread extends Thread {
        private final String url;
        private final String username;
        private final String password;
        private MultiHashMap transMap = new MultiHashMap();
        private int chunkSize;

        public StreamGeneratorThread(String url, String username, String password, MultiHashMap transMap) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.transMap = transMap;
        }

        private void processTransactions(Connection connection, MultiHashMap multiHashMap) throws SQLException {
            String transactionsQuery = "SELECT * FROM transactions";
            try (Statement transactionsStatement = connection.createStatement();
                 ResultSet transactionsResultSet = transactionsStatement.executeQuery(transactionsQuery)) {

                int count=chunkSize;
                while (transactionsResultSet.next()) {
                    if(count==0)
                        break;
                    int transProdId = transactionsResultSet.getInt("ProductID");
                    int orderId = transactionsResultSet.getInt("Order ID");
                    int custId = transactionsResultSet.getInt("CustomerID");
                    String custName = transactionsResultSet.getString("CustomerName");
                    String sex = transactionsResultSet.getString("Gender");
                    String orderDate = transactionsResultSet.getString("Order Date");
                    int quantityOrdered = transactionsResultSet.getInt("Quantity Ordered");

                    // Create a new Transaction instance
                    Transactions transaction = new Transactions(orderId, custId, custName, sex, orderDate, transProdId, quantityOrdered);

                    // Add this transaction to the MultiHashMap
                    multiHashMap.addTransaction(transProdId, transaction);
                    // Enqueue the join attribute (transProdId) into the queue
                    multiHashMap.enqueue(transProdId);
                }
            }
        }

        public void run() {
            try {
                Connection connection = DriverManager.getConnection(url, username, password);

                processTransactions(connection, transMap);

                transMap.printMultiValuedMap();
                transMap.printQueue();
                // Fetch and process data from master_data table
                String masterDataQuery = "SELECT * FROM master_data";
                Statement masterDataStatement = connection.createStatement();
                ResultSet masterDataResultSet = masterDataStatement.executeQuery(masterDataQuery);

                // Process data from master_data table
                while (masterDataResultSet.next()) {
                    // Retrieve data and perform necessary operations
                    int masterProdId = masterDataResultSet.getInt("productID");
                    String productName = masterDataResultSet.getString("productName");
                    String productPrice = masterDataResultSet.getString("productPrice");
                    int supplierId = masterDataResultSet.getInt("supplierID");
                    String supplierName = masterDataResultSet.getString("supplierName");
                    int storeId = masterDataResultSet.getInt("storeID");
                    String storeName = masterDataResultSet.getString("storeName");

                    MD masterData = new MD(masterProdId,productName,productPrice,supplierId,supplierName,storeId,storeName);
                    masterData.toString();

                }

                // Close resources
                masterDataResultSet.close();
                masterDataStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("SQL Exception: " + e.getMessage());
            }
        }
    }


    class HybridJoinThread extends Thread {
        private MultiHashMap multiHashMap;
        private final String url;
        private final String username;
        private final String password;
        private DiskBuffer buffer;
        private int counter;
        private int chunkSize;

        public HybridJoinThread(String url, String username, String password, MultiHashMap transMap) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.multiHashMap = transMap;
        }

        @Override
        public void run() {
            try {
                Connection connection = DriverManager.getConnection(url, username, password);

                chunkSize = 1000; // Initial chunk size
                counter = 1;

                loadInitialData(connection, chunkSize);

                while (!multiHashMap.isQueueEmpty()) {
                    // Read a new input chunk of sales data from the stream buffer
                    // Load it into the multi-hash table with their join attribute values in the queue
                    processChunk(connection, chunkSize);

                    int oldestNodeValue = multiHashMap.getOldestNodeValue();

                    loadMD(connection, oldestNodeValue);

                    matchAndLoad(connection);

                }

                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("SQL Exception: " + e.getMessage());
            }
        }

        private void loadInitialData(Connection connection, int chunkSize) throws SQLException {
            // Load initial data into the Hash table and Queue


        }

        private void processChunk(Connection connection, int chunkSize) throws SQLException {
            // Process a new chunk of data into the Hash table and Queue
        }

        private void loadMD(Connection connection, int oldestNode) throws SQLException {
            int oldestNodeValue = multiHashMap.getOldestNodeValue();
            buffer.loadSegment(oldestNodeValue, connection);


        }

        public void setChunkSize(int size){
            chunkSize=size;
        }

        private void matchAndLoad(Connection connection) throws SQLException {
            List<MD> buffer = this.buffer.getBuffer();
            for (MD md : buffer) {
                int joinAttributeValue = md.getProductID();
                Collection<Transactions> transactions = multiHashMap.getTransactions(joinAttributeValue);
                if (transactions != null && !transactions.isEmpty()) {
                    for (Transactions transaction : transactions) {
                        counter++;

                        // Calculate TOTAL_SALE using QUANTITY and PRICE attributes
                        double totalSale = transaction.getQuantity() * md.getPrice();

                        /////////////LOAD INTO DATABASE////////////////////
                        //transaction.setTotalSale(totalSale);
                        if (counter <= 50) {
                            System.out.println("Output Tuple: " + counter + " " + transaction); // Print output tuple
                        }
                        multiHashMap.removeTransactions(joinAttributeValue);
                    }
                }
            }
        }
    }
    class ControllerThread extends Thread {
    private StreamGeneratorThread streamGenerator;
    private HybridJoinThread hybridJoin;
    int arrivalRate;
    int serviceRate;

    public ControllerThread(StreamGeneratorThread streamGenerator, HybridJoinThread hybridJoin) {
        this.streamGenerator = streamGenerator;
        this.hybridJoin = hybridJoin;
        arrivalRate=1000;
        serviceRate=0;
    }

    @Override
    public void run() {
        try {
            // Control the speed of StreamGenerator based on service rate


            while (true) {
                // Monitor arrival rate and service rate
                // Calculate and adjust the pace of StreamGenerator

                // Adjust service rates or resource allocation based on performance metrics

                //  regulate HybridJoinThread based on system load or data arrival rate

                Thread.sleep(1000 );
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}


    //making transaction class to store transactions data and then map into hashtable
    class Transactions {
        private int orderId;
        private int custId;
        private String custName;
        private String sex;
        private String orderDate;
        private int transProdId;
        private int quantityOrdered;

        public Transactions(int orderId, int custId, String custName, String sex, String orderDate, int transProdId, int quantityOrdered) {
            this.orderId = orderId;
            this.custId = custId;
            this.custName = custName;
            this.sex = sex;
            this.orderDate = orderDate;
            this.transProdId = transProdId;
            this.quantityOrdered = quantityOrdered;
        }

        // Getters and setters
        public int getTransProdId() {
            return transProdId;
        }

        public int getQuantity( ){return quantityOrdered;}

        public void setTransProdId(int transProdId) {
            this.transProdId = transProdId;
        }


        @Override
        public String toString() {
            return "Transaction{" + ", " +
                    "orderId=" + orderId +
                    ", custId=" + custId +
                    ", custName='" + custName + '\'' +
                    ", sex='" + sex + '\'' +
                    ", orderDate='" + orderDate + '\'' +
                    ", transProdId=" + transProdId +
                    ", quantityOrdered=" + quantityOrdered +
                    '}';
        }
    }

    //Master data class
    class MD {
        private int productID;
        private String productName;
        private String productPrice;
        private int supplierID;
        private String supplierName;
        private int storeID;
        private String storeName;

        // Constructor
        public MD(int productID, String productName, String productPrice, int supplierID,
                  String supplierName, int storeID, String storeName) {
            this.productID = productID;
            this.productName = productName;
            this.productPrice = productPrice;
            this.supplierID = supplierID;
            this.supplierName = supplierName;
            this.storeID = storeID;
            this.storeName = storeName;
        }

        // Getter and setter
        public int getProductID() {
            return productID;
        }

        public void setProductID(int productID) {
            this.productID = productID;
        }

        public double getPrice(){return parseProductPrice();}

        @Override
        public String toString() {
            return "MD{" +
                    "productID=" + productID +
                    ", productName='" + productName + '\'' +
                    ", productPrice='" + productPrice + '\'' +
                    ", supplierID=" + supplierID +
                    ", supplierName='" + supplierName + '\'' +
                    ", storeID=" + storeID +
                    ", storeName='" + storeName + '\'' +
                    '}';
        }
        public double parseProductPrice() {
            // Remove the '$' sign from the productPrice
            String priceWithoutSymbol = productPrice.replace("$", "");

            // Parse the String to double
            try {
                return Double.parseDouble(priceWithoutSymbol);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return 0.0;
            }
        }

    }

    class MultiHashMap {
        private MultiValuedMap<Integer, Transactions> transactionMap = new HashSetValuedHashMap<>();
        private DoublyLinkedList queue = new DoublyLinkedList();

        public void addTransaction(int key, Transactions transaction) {
            transactionMap.put(key, transaction);
            queue.add(key);
        }

        public Collection<Transactions> getTransactions(int key) {
            return transactionMap.get(key);
        }

        public void removeTransactions(int key) {
            transactionMap.remove(key);
            queue.remove(key);
        }

        public void printQueue() {
            queue.printList();
        }

        public void printMultiValuedMap() {
            for (Map.Entry<Integer, Transactions> entry : transactionMap.entries()) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }

        public void enqueue(int key) {
            queue.add(key);
        }

        public int getOldestNodeValue(){
            return queue.getHeadValue();
        }

        public boolean isQueueEmpty(){
            return isQueueEmpty();
        }
    }

    class QueueNode {
        int productID; // The join attribute value
        QueueNode prev; // Pointer to the previous node
        QueueNode next; // Pointer to the next node

        public QueueNode(int productID) {
            this.productID = productID;
            this.prev = null;
            this.next = null;
        }

        //print node
        public void printNode() {
            System.out.println(productID);
        }

        //get node
        public int getNode() {
            return productID;
        }
    }

    class DoublyLinkedList {
        private QueueNode head;
        private QueueNode tail;

        public DoublyLinkedList() {
            this.head = null;
            this.tail = null;
        }

        // Method to add productID to the end of the queue
        public QueueNode add(int prodID) {
            QueueNode newNode = new QueueNode(prodID);
            if (head == null) {
                head = newNode;
                tail = newNode;
            } else {
                tail.next = newNode;
                newNode.prev = tail;
                tail = newNode;
            }
            return newNode;
        }

        // Method to remove a specific productID from the queue
        public void remove(int prodID) {
            QueueNode current = head;
            while (current != null) {
                if (current.productID == prodID) {
                    if (current == head) {
                        head = head.next;
                        if (head != null) {
                            head.prev = null;
                        }
                    } else if (current == tail) {
                        tail = tail.prev;
                        tail.next = null;
                    } else {
                        current.prev.next = current.next;
                        current.next.prev = current.prev;
                    }
                    break;
                }
                current = current.next;
            }
        }

        public void printList() {
            QueueNode current = head;
            while (current != null) {
                System.out.print(current.getNode() + " <--> "); // Print the node's value
                current = current.next; // Move to the next node
            }
            System.out.println("null"); // Indicate the end of the list
        }

        // Method to check if the queue is empty
        public boolean isEmpty() {
            return head == null;
        }

        public int getHeadValue() {
            if (head != null) {
                return head.productID;
            }
            return -1; // Return a default value if the head is null
        }
    }

    class DiskBuffer {
        private List<MD> buffer; // Store MD objects

        public DiskBuffer() {
            this.buffer = new ArrayList<>(); // Initialize buffer as an ArrayList
        }

        // Method to load a segment of MD tuples into the disk-buffer using the oldest node as an index
        public void loadSegment(int oldestNodeValue, Connection connection) throws SQLException {
            String mdQuery = "SELECT * FROM master_data WHERE join_attribute = ? LIMIT 10"; // SQL query to fetch MD tuples

            try (PreparedStatement statement = connection.prepareStatement(mdQuery)) {
                statement.setInt(1, oldestNodeValue); // Set the join attribute value

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        int productID = resultSet.getInt("productID");
                        String productName = resultSet.getString("productName");
                        String productPrice = resultSet.getString("productPrice");
                        int supplierID = resultSet.getInt("supplierID");
                        String supplierName = resultSet.getString("supplierName");
                        int storeID = resultSet.getInt("storeID");
                        String storeName = resultSet.getString("storeName");

                        // Create an MD object with fetched data
                        MD mdObject = new MD(productID, productName, productPrice, supplierID, supplierName, storeID, storeName);
                        buffer.add(mdObject); // Add MD object to the buffer
                    }
                }
            }
        }


        // retrieve the MD tuples from the disk-buffer
        public List<MD> getBuffer() {
            return buffer;
        }
    }

