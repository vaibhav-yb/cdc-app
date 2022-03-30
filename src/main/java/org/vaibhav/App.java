package org.vaibhav;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.vaibhav.tables.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

// mvn exec:java -Dexec.mainClass=org.vaibhav.App -Dexec.args="192.168.1.51"

public class App {
  private static final Path resourcePath = Paths.get("src", "main", "resources");
  private static final String RESOURCES_PATH = resourcePath.toFile().getAbsolutePath();
  private static final long TEN_MILLION = 10000000;

  public static void runSqlScript(Connection conn, String fileName) {
    System.out.println("Running the SQL script: " + fileName);
    ScriptRunner sr = new ScriptRunner(conn);
    sr.setAutoCommit(true);

    // This prevents ScriptWriter from printing the queries on terminal or logging them either.
//    sr.setLogWriter(null);

    final String sqlFile = RESOURCES_PATH + "/"+ fileName;
    try {
      Reader reader = new BufferedReader(new FileReader(sqlFile));
      sr.runScript(reader);
    } catch (FileNotFoundException f) {
      f.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // args will contain the endpoints
    if (args.length == 0) {
      System.out.println("No endpoints specified to connect to, exiting...");
      System.exit(-1);
    }

    try {
      Connection conn = DriverManager.getConnection("jdbc:yugabytedb://" + args[0] + ":5433/yugabyte?" +
              "user=yugabyte&password=yugabyte");
      runSqlScript(conn, "create_tables.sql");
      System.out.println("Tables created, waiting for 5 seconds...");
      conn.close();
      Thread.sleep(5000);
    } catch (Exception se) {
      System.out.println("Exception thrown: " + se);
      System.exit(-2);
    }

    int numThreads = 18;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    while (true) {
      try {
        Future f1_1 = executorService.submit(new ApiAppleStoreReceiptLog(args, 1, TEN_MILLION));
        Future f1_2 = executorService.submit(new ApiAppleStoreReceiptLog(args, TEN_MILLION, 2*TEN_MILLION));
        Future f1_3 = executorService.submit(new ApiAppleStoreReceiptLog(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f2_1 = executorService.submit(new ApiOauthAccessToken(args, 1, TEN_MILLION));
        Future f2_2 = executorService.submit(new ApiOauthAccessToken(args, TEN_MILLION, 2*TEN_MILLION));
        Future f2_3 = executorService.submit(new ApiOauthAccessToken(args, 2*TEN_MILLION, 3*TEN_MILLION));
        
        Future f3_1 = executorService.submit(new ApiSubAppleOrigTransactions(args, 1, TEN_MILLION));
        Future f3_2 = executorService.submit(new ApiSubAppleOrigTransactions(args, TEN_MILLION, 2*TEN_MILLION));
        Future f3_3 = executorService.submit(new ApiSubAppleOrigTransactions(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f4_1 = executorService.submit(new ApiSubRecurlyNotifyLog(args, 1, TEN_MILLION));
        Future f4_2 = executorService.submit(new ApiSubRecurlyNotifyLog(args, TEN_MILLION, 2*TEN_MILLION));
        Future f4_3 = executorService.submit(new ApiSubRecurlyNotifyLog(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f5_1 = executorService.submit(new ApiSubUserPackages(args, 1, TEN_MILLION));
        Future f5_2 = executorService.submit(new ApiSubUserPackages(args, TEN_MILLION, 2*TEN_MILLION));
        Future f5_3 = executorService.submit(new ApiSubUserPackages(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f6_1 = executorService.submit(new ApiUserPartnerTransactionLog(args, 1, TEN_MILLION));
        Future f6_2 = executorService.submit(new ApiUserPartnerTransactionLog(args, TEN_MILLION, 2*TEN_MILLION));
        Future f6_3 = executorService.submit(new ApiUserPartnerTransactionLog(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f7_1 = executorService.submit(new ApiUserDevice(args, 1, TEN_MILLION));
        Future f7_2 = executorService.submit(new ApiUserDevice(args, TEN_MILLION, 2*TEN_MILLION));
        Future f7_3 = executorService.submit(new ApiUserDevice(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f8_1 = executorService.submit(new ApiWatchList(args, 1, TEN_MILLION));
        Future f8_2 = executorService.submit(new ApiWatchList(args, TEN_MILLION, 2*TEN_MILLION));
        Future f8_3 = executorService.submit(new ApiWatchList(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f9_1 = executorService.submit(new ApiWatchList(args, 1, TEN_MILLION));
        Future f9_2 = executorService.submit(new ApiWatchList(args, TEN_MILLION, 2*TEN_MILLION));
        Future f9_3 = executorService.submit(new ApiWatchList(args, 2*TEN_MILLION, 3*TEN_MILLION));

        Future f10_1 = executorService.submit(new ApiWatchList(args, 1, TEN_MILLION));
        Future f10_2 = executorService.submit(new ApiWatchList(args, TEN_MILLION, 2*TEN_MILLION));
        Future f10_3 = executorService.submit(new ApiWatchList(args, 2*TEN_MILLION, 3*TEN_MILLION));

        f1_1.get();
        f1_2.get();
        f1_3.get();

        f2_1.get();
        f2_2.get();
        f2_3.get();

        f3_1.get();
        f3_2.get();
        f3_3.get();

        f4_1.get();
        f4_2.get();
        f4_3.get();

        f5_1.get();
        f5_2.get();
        f5_3.get();

        f6_1.get();
        f6_2.get();
        f6_3.get();

        f7_1.get();
        f7_2.get();
        f7_3.get();

        f8_1.get();
        f8_2.get();
        f8_3.get();

        f9_1.get();
        f9_2.get();
        f9_3.get();

        f10_1.get();
        f10_2.get();
        f10_3.get();
      } catch (Exception e) {
        System.out.println("Exception thrown in main application...");
      }
    }
  }
}
