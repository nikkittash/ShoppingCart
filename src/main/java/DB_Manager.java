import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Arrays;
import java.util.function.Consumer;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class DB_Manager {

    private final MongoCollection<Document> PRODUCTS;
    private final MongoCollection<Document> SHOPS;

    public DB_Manager() {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("local");
        PRODUCTS = mongoDatabase.getCollection("products");
        SHOPS = mongoDatabase.getCollection("shops");
    }

    public MongoCollection<Document> getPRODUCTS() {
        return PRODUCTS;
    }

    public MongoCollection<Document> getSHOPS() {
        return SHOPS;
    }

    public void addProduct(String name, int price) {
        getPRODUCTS().insertOne(new Document("name", name).append("price", price));
        System.out.println("The product was successfully added!");
    }

    public void addShop(String name) {
        getSHOPS().insertOne(new Document("name", name));
        System.out.println("The shop was successfully added!");
    }

    public void postProduct(String nameProduct, String nameShop) {
        StringBuilder product = new StringBuilder().append("{$push : {products : \"").
                append(nameProduct).append("\"}}");
        StringBuilder shop = new StringBuilder().append("{name : \"").append(nameShop).append("\"}");
        getSHOPS().updateOne(BsonDocument.parse(shop.toString()), BsonDocument.parse(product.toString()));
        System.out.println("The product was successfully post in the shop!");
    }

    public void getStatistics() {
        getAggregation().forEach((Consumer<Document>) doc -> {
            String shop = doc.getString("_id");
            int countCommon = doc.getInteger("countCommon");
            double avgPrice = doc.getDouble("avgPrice");
            int maxPrice = doc.getInteger("maxPrice");
            int minPrice = doc.getInteger("minPrice");
            int countLt100 = doc.getInteger("countLt");

            System.out.printf("----- Statistic for store <%s> -----\n"
                            + "\tNumber of products - %s\n"
                            + "\tAverage price of products - %s\n"
                            + "\tThe most expensive product - %s\n"
                            + "\tThe most Cheapest product - %s\n"
                            + "\tThe Number of products is less than 100 rub - %s\n", shop, countCommon, avgPrice, maxPrice,
                    minPrice, countLt100);
        });
    }

    public AggregateIterable<Document> getAggregation() {
        return getSHOPS().aggregate(
                Arrays.asList(
                        unwind("$products"),
                        lookup("products", "products", "name", "products_list"),
                        addFields(new Field<>("count", 1)),
                        unwind("$products_list"),
                        addFields(new Field<>("priceFirst", "$products_list.price")),
                        project(fields(include("name", "products", "count", "priceFirst"))),
                        group("$name",
                                sum("count", "$count"),
                                avg("avgPrice", "$priceFirst"),
                                min("minPrice", "$priceFirst"),
                                max("maxPrice", "$priceFirst"),
                                push("products", "$priceFirst")),
                        unwind("$products"),
                        match(lt("products", 100)),
                        addFields(new Field<>("countLt", 1)),
                        group("$_id",
                                sum("countLt", "$countLt"),
                                first("countCommon", "$count"),
                                first("avgPrice", "$avgPrice"),
                                first("minPrice", "$minPrice"),
                                first("maxPrice", "$maxPrice"))
                ));
    }
}
