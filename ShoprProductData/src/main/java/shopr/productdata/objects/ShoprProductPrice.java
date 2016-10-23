package shopr.productdata.objects;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.sql.Date;

/**
 * Created by Neil on 10/22/2016.
 *
 * @author Neil Allison
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({"ds", "upc", "regularPrice", "salePrice", "vendor"})
public class ShoprProductPrice
{
    private Date ds;
    private String upc;
    private Double regularPrice;
    private Double salePrice;
    private String vendor;

    public ShoprProductPrice()
    {
        // Explicit default needed for Jackson
    }

    public Date getDs()
    {
        return ds;
    }

    public void setDs(Date ds)
    {
        this.ds = ds;
    }

    public String getUpc()
    {
        return upc;
    }

    public void setUpc(String upc)
    {
        this.upc = upc;
    }

    public Double getRegularPrice()
    {
        return regularPrice;
    }

    public void setRegularPrice(Double regularPrice)
    {
        this.regularPrice = regularPrice;
    }

    public Double getSalePrice()
    {
        return salePrice;
    }

    public void setSalePrice(Double salePrice)
    {
        this.salePrice = salePrice;
    }

    public String getVendor()
    {
        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }
}
