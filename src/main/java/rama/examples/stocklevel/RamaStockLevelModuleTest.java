package rama.examples.stocklevel;

import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class RamaStockLevelModuleTest {

    @Test
    public void basicTest() throws Exception {
        try (InProcessCluster ipc = InProcessCluster.create()) {
            RamaStockLevelModule stockLevelModule = new RamaStockLevelModule();
            String moduleName = stockLevelModule.getClass().getName();
            ipc.launchModule(stockLevelModule, new LaunchConfig(4, 4));
            RamaStockLevelClient client = new RamaStockLevelClient(ipc);


            // set initial stock levels:
            client.appendStockLevelRecord("apple", 10);
            client.appendStockLevelRecord("banana", 20);
            client.appendStockLevelRecord("cherry", 30);

            ipc.waitForMicrobatchProcessedCount(moduleName, "stocklevel", 3);

            assertEquals(client.getStockLevelRecord("apple").stockLevel, 10);
            assertEquals(client.getStockLevelRecord("banana").stockLevel, 20);
            assertEquals(client.getStockLevelRecord("cherry").stockLevel, 30);

            // process reservation:
            client.appendStockReservation("order1",
                    new HashMap<String, Integer>() {
                        {
                            put("apple", 1);
                            put("banana", 2);
                            put("cherry", 3);
                        }
                    }
            );
            ipc.waitForMicrobatchProcessedCount(moduleName, "stocklevel", 4);

            assertEquals(client.getStockLevelRecord("apple").stockLevel, 10 - 1);
            assertEquals(client.getStockLevelRecord("banana").stockLevel, 20 - 2);
            assertEquals(client.getStockLevelRecord("cherry").stockLevel, 30 - 3);

        }
    }
}
