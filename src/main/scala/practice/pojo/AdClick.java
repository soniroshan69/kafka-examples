
package practice.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "InventoryID"
})
public class AdClick {

    @JsonProperty("InventoryID")
    private String inventoryID;

    @JsonProperty("InventoryID")
    public String getInventoryID() {
        return inventoryID;
    }

    @JsonProperty("InventoryID")
    public void setInventoryID(String inventoryID) {
        this.inventoryID = inventoryID;
    }

    public AdClick withInventoryID(String inventoryID) {
        this.inventoryID = inventoryID;
        return this;
    }

}
