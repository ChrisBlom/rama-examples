package rama.examples.stocklevel;

import com.rpl.rama.Depot;
import com.rpl.rama.PState;
import com.rpl.rama.Path;
import com.rpl.rama.RamaModule;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction2;

import static com.rpl.rama.helpers.TopologyUtils.extractJavaFields;

public class RamaStockLevelModule implements RamaModule {

    private static void declareStockLevelTopology(Topologies topologies) {
        MicrobatchTopology topology = topologies.microbatch("stocklevel");

        topology.pstate(
                "$$stockLevels",
                PState.mapSchema(
                        String.class, // indexed by productId
                        PState.fixedKeysSchema(
                                "productId", String.class,
                                "stockLevel", Integer.class)
                ));

        // store incoming stock level records (replacing existing levels)
        topology.source("*stockLevelDepot").out("*batch")
                .explodeMicrobatch("*batch").out("*record")
                .macro(extractJavaFields("*record", "*productId", "*stockLevel"))
                .localTransform("$$stockLevels",
                        Path.key("*productId")
                                .multiPath(
                                        Path.key("productId").termVal("*productId"),
                                        Path.key("stockLevel").termVal("*stockLevel")
                                ));

        // when a reservation arrives, subtract the reserved amount from the stock levels
        topology.source("*reservationDepot").out("*batch")
                .explodeMicrobatch("*batch").out("*reservation")
                .macro(extractJavaFields("*reservation", "*orderId", "*lines"))
                .each(Ops.EXPLODE, "*lines").out("*line")
                .macro(extractJavaFields("*line", "*productId", "*quantity"))
                .hashPartition("*productId")
                .localTransform("$$stockLevels",
                        Path.key("*productId", "stockLevel").term((RamaFunction2<Integer, Integer, Integer>)
                                        (stockLevel, quantity) -> stockLevel - quantity,
                                "*quantity")
                ).e;

    }

    public static class ProductIdExtract extends TopologyUtils.ExtractJavaField {
        public ProductIdExtract() {
            super("productId");
        }
    }

    public static class OrderIdExtract extends TopologyUtils.ExtractJavaField {
        public OrderIdExtract() {
            super("orderId");
        }
    }

    @Override
    public void define(Setup setup, Topologies topologies) {
        setup.declareDepot("*stockLevelDepot", Depot.hashBy(ProductIdExtract.class));
        setup.declareDepot("*reservationDepot", Depot.hashBy(OrderIdExtract.class));

        declareStockLevelTopology(topologies);

    }
}
