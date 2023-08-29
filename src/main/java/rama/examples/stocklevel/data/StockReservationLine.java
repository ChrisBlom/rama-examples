package rama.examples.stocklevel.data;

import com.rpl.rama.RamaSerializable;

import java.util.StringJoiner;

public class StockReservationLine implements RamaSerializable {
    public String productId;
    public int quantity;

    public StockReservationLine(String productId, int quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StockReservationLine.class.getSimpleName() + "[", "]")
                .add("productId='" + productId + "'")
                .add("quantity=" + quantity)
                .toString();
    }
}
