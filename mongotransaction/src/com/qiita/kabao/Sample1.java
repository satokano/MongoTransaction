/**
 * MongoDB 4.0 Multi-Document Transactionのサンプル
 * https://qiita.com/kabao/items/ecc8e97b2995227f47e8
 */
package com.qiita.kabao;

import java.util.Date;

import org.bson.Document;

import com.mongodb.ClientSessionOptions;
// New MongoClient API (since 3.7)
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * @author okano
 *
 */
public class Sample1 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Connection Stringとflagが読み取れませんでした");
			return;
		}
		
		String connectionString = args[0];
		String flagValue = args[1];
		
		MongoClient mongoClient = MongoClients.create(connectionString);
		MongoDatabase mongoDatabase = mongoClient.getDatabase("transactiondb");
		MongoCollection<Document> collection = mongoDatabase.getCollection("sample1");

		/**
		 * トランザクション処理。
		 * 
		 * insertなどのオペレーションが正常に完了した場合commitTransaction、close
		 * 異常時はabortTransaction、closeとする。
		 * 
		 * ClientSessionはAutoCloseableを実装していて、try-with-resourcesも使えるのだが
		 * try-with-resourcesにすると例外発生時に、自動close処理→catch→finallyの順となり
		 * finallyにabortを書いたとしてもclose→abortの順となってしまって意味が無い。
		 *   https://docs.oracle.com/javase/jp/7/technotes/guides/language/try-with-resources.html
		 *   > try-with-resources 文内の catch または finally ブロックは、宣言されているリソースが閉じられたあとで実行されます。
		 *   
		 * なのでtry-with-resourcesとせずに、従来の形で書く。
		 * aliceWantsTwoExtraBeersInTransactionThenCommitOrRollbackメソッドを参考にした。
		 * https://github.com/MaBeuLux88/mongodb-4.0-demos/blob/master/src/main/java/com/mongodb/Transactions.java#L89
		 * 
		 * commitTransaction自体、ドライバの機能により、1回リトライが働く（retryWritesの設定に関わらず）
		 */
		System.err.println("Start Session");
		// Causal Consistent Sessionにしておく
		ClientSession clientSession = mongoClient.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());
		try {
			System.err.println("Start Transaction");
			// TransactionではRead Concernをsnapshot、Write Concernをmajorityにしておく。
			// これにより、トランザクション開始直前のオペレーションとCausal Consistencyが保たれるようになる。
			// TransactionのRead Concernは、トランザクション開始時に指定、またはSessionから引継ぎ、またはClientから引継ぎ。（DatabaseやCollectionは見ない）
			// TransactionのWrite Concernは、トランザクション開始時に指定のみ。
			// https://docs.mongodb.com/master/core/transactions/#transaction-options
			clientSession.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build());
			Document doc1 = new Document("name", "satoshi")
					.append("address", new Document("country", "日本").append("pref", "神奈川").append("city", "横浜").append("zipcode", "220-0001"))
					.append("lastModified", new Date());
			collection.replaceOne(clientSession, eq("name", "satoshi"), doc1, new ReplaceOptions().upsert(true)); // MongoWriteException, MongoWriteConcernException
			
			Document doc2 = new Document("name", "vigyan")
					.append("address", new Document("country", "Australia").append("state", "VIC").append("city", "Melbourne").append("street", "120 Collins Street").append("postcode", "3000"))
					.append("lastModified", new Date());
			collection.replaceOne(clientSession, eq("name", "vigyan"), doc2, new ReplaceOptions().upsert(true)); // MongoWriteException, MongoWriteConcernException
			
			Document doc3 = collection.findOneAndUpdate(clientSession, eq("name", "satoshi"), combine(set("flag", flagValue), currentDate("lastModified")));
			System.err.println(doc3.toJson());
			
			try {
				Thread.sleep(70000);
			} catch (InterruptedException ie) {
				throw new RuntimeException(ie);
			}
			
			// ドライバが1回はリトライしてくれる（retryWritesの設定に関わらず）
			// MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL のラベルを持つ例外が出たときは再実行可能だが
			// 1回のリトライで十分と考えて、そこまでは再実行しないこととする。
			System.err.println("Commit....");
			clientSession.commitTransaction();
			System.err.println("....done");
			
			// MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL のラベルを持つ例外はトランザクション全体に対するもので
			// リトライロジックを書こうとすると複雑になる（トランザクション単位できれいにメソッド化する等）。
			// ドライバによるcommitのリトライのみで十分と考えて、トランザクション単位のリトライはしないこととする。
			
		} catch (MongoException me) {
			System.err.println("MongoException");
			System.err.println("Abort....");
			clientSession.abortTransaction();
			System.err.println("....done");
			me.printStackTrace(System.err);
		} finally {
			System.err.println("close");
			clientSession.close(); // チェック例外は無いようだが、RuntimeExceptionは？
		}
		
		System.err.println("end");

	}

}
