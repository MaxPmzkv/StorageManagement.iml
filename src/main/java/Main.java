import java.util.Scanner;

public class Main
{
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 27017;
    private static final String DB = "test";

    public static void main(String[] args) {
        GoodsManagement management = GoodsManagement.getInstance(HOST, PORT, DB);

        System.out.println("Command example:");
        System.out.println(
                "add_store storeName" +
                        "\nadd_product productName productPrice" +
                        "\nexhibit productName storeName" +
                        "\nlist to see statistics" +
                        "\nexit");
        Scanner scanner = new Scanner(System.in);
        while(true){
            String command = scanner.nextLine();
            if(management.isCommandExit(command)){
                System.out.println("Goodbye!");
                break;
            }
            try{
                if(management.isCommandAddStore(command)){
                    management.addStore(command);
                }
                if(management.isCommandAddProduct(command)){
                   management.addProduct(command);
                }
                if(management.isCommandExhibit(command)){
                    management.exhibitProductToStore(command);
                }
                if(management.isCommandList(command)){
                    management.listStatistics();
                }
            }
            catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }
}