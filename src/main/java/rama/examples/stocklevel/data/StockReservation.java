package rama.examples.stocklevel.data;

import com.rpl.rama.RamaSerializable;

import java.util.List;
import java.util.StringJoiner;

public class StockReservation implements RamaSerializable {
    public String orderId;
    public List<StockReservationLine> lines;

    public StockReservation(String orderId, List<StockReservationLine> lines) {
        this.orderId = orderId;
        this.lines = lines;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", StockReservation.class.getSimpleName() + "[", "]")
                .add("orderId='" + orderId + "'")
                .add("orderLines=" + lines)
                .toString();
    }
}
