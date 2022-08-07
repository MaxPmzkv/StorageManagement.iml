import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;

import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class GoodsManagement {
    private static volatile GoodsManagement instance;

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> stores;
    private MongoCollection<Document> products;

    private static final String STORES_COLLECTION = "stores";
    private static final String PRODUCTS_COLLECTION = "products";

    private static final String ADD_STORE_COMMAND = "add_store";
    private static final String ADD_PRODUCT_COMMAND = "add_product";
    private static final String EXHIBIT_PRODUCT_COMMAND = "exhibit";
    private static final String COMMAND_LIST = "list";
    private static final String COMMAND_EXIT = "exit";

    private static final String FIELD_STORE = "Store";
    private static final String FIELD_PRODUCTS = "Products";
    private static final String FIELD_PRODUCT = "Product";
    private static final String FIELD_PRICE = "Price";



    private static final int COMPARISON_PRICE = 100;


    private GoodsManagement(String HOST, int PORT, String DB) {
        client = new MongoClient(HOST, PORT);
        database = client.getDatabase(DB);
        stores = database.getCollection(STORES_COLLECTION);
        products = database.getCollection(PRODUCTS_COLLECTION);
    }

    public static GoodsManagement getInstance(String HOST, int PORT, String DB){
        GoodsManagement result = instance;
        if(result != null){
            return result;
        }
        synchronized (GoodsManagement.class){
            if(instance == null){
                instance = new GoodsManagement(HOST, PORT, DB);
            }
            return instance;
        }
    }



    public void addStore(String command) {
        String storeName = command.replaceAll(ADD_STORE_COMMAND, "").trim();
        if(!storeExists(storeName)){
            Document store = new Document().append(FIELD_STORE, storeName)
                    .append(FIELD_PRODUCTS, new ArrayList<String>());
            stores.insertOne(store);
            System.out.println("Store " + storeName + " has been added!");
        }
        else{
            System.err.println("This store has been already added!");
        }
    }

    public void addProduct(String command) {
        if(isCommandAddProduct(command)){
            int productName = 0;
            int productPrice = 1;
            String[] commandArray = command.replaceAll(ADD_PRODUCT_COMMAND, "")
                    .trim().split(" ");
            if(!productExists(commandArray[productName])){
                Document product = new Document().append(FIELD_PRODUCT, commandArray[productName])
                        .append(FIELD_PRICE, Integer.valueOf(commandArray[productPrice]));
                products.insertOne(product);
                System.out.println("Product " + commandArray[productName]
                        + " with the price of << " + commandArray[productPrice]
                        + " >> has been added");
            }
            else{
                System.err.println("This product has been already added!");
            }
        }
    }

    public void exhibitProductToStore(String command) {
        int productName = 0;
        int storeName = 1;
        String[] commandArray = command.replaceAll(EXHIBIT_PRODUCT_COMMAND, "").trim().split(" ");
        if (productExists(commandArray[productName]) && storeExists(commandArray[storeName])) {
            Document store = stores.find(eq("Store", commandArray[storeName])).first();
            ArrayList<String> productsList = (ArrayList<String>) store.get("Products");

            if (!productsList.contains(commandArray[productName])) {
                productsList.add(commandArray[productName]);
            }
            stores.findOneAndUpdate(eq("Store", commandArray[storeName]), new Document("$set", new Document("Products", productsList)));
            System.out.println("Product " + commandArray[productName] + " added to store " + commandArray[storeName]);
        }
        else{
            System.err.println("No such product or store!");
        }
    }

    private boolean productExists(String productName) {
        return products.find(eq(FIELD_PRODUCT, productName)).first() != null;
    }

    private boolean storeExists (String storeName) {
        return stores.find(eq(FIELD_STORE, storeName)).first() != null;
    }
    public boolean isCommandExit(String command) {
        return (command.contains(COMMAND_EXIT) && (command.trim().split(" ").length == 1));
    }

    public boolean isCommandAddStore(String command) {
        return (command.contains(ADD_STORE_COMMAND) && (command.trim().split(" ").length == 2));
    }

    public boolean isCommandAddProduct(String command) {
        return (command.contains(ADD_PRODUCT_COMMAND) && (command.trim().split(" ").length == 3));
    }

    public boolean isCommandExhibit(String command) {
        return (command.contains(EXHIBIT_PRODUCT_COMMAND) && (command.trim().split(" ").length == 3));
    }

    public boolean isCommandList(String command) {
        return (command.contains(COMMAND_LIST) && (command.trim().split(" ").length == 1));
    }

    public void listStatistics() {
        AggregateIterable<Document> statistics = stores
                .aggregate(Arrays.asList(
                        Aggregates.lookup(products.getNamespace().getCollectionName(), "Products", "Product", "Products"),
                        Aggregates.unwind("$Products"),
                        Aggregates.group("$Store", Accumulators.sum("Total products amount", 1),
                                Accumulators.avg("AvgPrice", "$Products.Price"),
                                Accumulators.max("MaxPrice", "$Products.Price"),
                                Accumulators.min("MinPrice", "$Products.Price"))
                ));
        AggregateIterable<Document> numberOfProductsCheaperComparisonPrice = stores
                .aggregate(Arrays.asList(
                        Aggregates.lookup(products.getNamespace().getCollectionName(), "Products", "Product", "Products"),
                        Aggregates.unwind("$Products"),
                        Aggregates.match(lt("Products.Price", COMPARISON_PRICE)),
                        Aggregates.group("$Store", Accumulators.sum
                                ("Amount of products, cheaper then 100 ", 1))
                ));
        System.out.println("Statistics for each store: ");

        statistics.forEach((Consumer<Document>) doc ->
                System.out.println(doc.toJson(JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build())));
        numberOfProductsCheaperComparisonPrice.forEach((Consumer<Document>) doc ->
                System.out.println(doc.toJson(JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build())));
    }



}



