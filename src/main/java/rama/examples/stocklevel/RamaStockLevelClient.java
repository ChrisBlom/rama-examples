package rama.examples.stocklevel;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.ProxyState;
import com.rpl.rama.cluster.ClusterManagerBase;
import com.rpl.rama.diffs.Diff;
import rama.examples.stocklevel.data.StockLevelRecord;
import rama.examples.stocklevel.data.StockReservation;
import rama.examples.stocklevel.data.StockReservationLine;

import java.util.Map;
import java.util.stream.Collectors;

public class RamaStockLevelClient {
    private Depot _reservationDepot;
    private Depot _stockLevelDepot;
    private PState _stockLevelState;

    public RamaStockLevelClient(ClusterManagerBase cluster) {
        String moduleName = RamaStockLevelModule.class.getName();
        _stockLevelDepot = cluster.clusterDepot(moduleName, "*stockLevelDepot");
        _reservationDepot = cluster.clusterDepot(moduleName, "*reservationDepot");
        _stockLevelState = cluster.clusterPState(moduleName, "$$stockLevels");

        _stockLevelState.proxy(Path.all().key("stockLevel"), new ProxyState.Callback<Object>() {
            @Override
            public void change(Object newVal, Diff diff, Object oldVal) {
                System.out.println("apple changed: " + oldVal + " -> " + newVal + " diff: " + diff);
            }

        });


    }


    public void appendStockLevelRecord(String productId, int quantity) {
        _stockLevelDepot.append(new StockLevelRecord(productId, quantity));
    }

    public void appendStockReservation(String orderId, Map<String, Integer> lines) {
        _reservationDepot.append(new StockReservation(orderId, lines.entrySet().stream().map((e -> new StockReservationLine(e.getKey(), e.getValue()))).collect(Collectors.toList())));
    }

    public StockLevelRecord getStockLevelRecord(String id) {
        Map record = _stockLevelState.selectOne(Path.key(id));
        return new StockLevelRecord((String) record.get("productId"), (Integer) record.get("stockLevel"));
    }

}
