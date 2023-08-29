package rama.examples.stocklevel.data;

import com.rpl.rama.RamaSerializable;

import java.util.StringJoiner;

public class StockLevelRecord implements RamaSerializable {
    public String productId;
    public int stockLevel;

    public StockLevelRecord(String id, int stockLevel) {
        this.productId = id;
        this.stockLevel = stockLevel;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StockLevelRecord.class.getSimpleName() + "[", "]")
                .add("productId='" + productId + "'")
                .add("stockLevel=" + stockLevel)
                .toString();
    }
}
