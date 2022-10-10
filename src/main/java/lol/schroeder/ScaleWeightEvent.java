package lol.schroeder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScaleWeightEvent {
    private String deviceId;
    private String sensor;
    private Double value;
    private Long time;
}
