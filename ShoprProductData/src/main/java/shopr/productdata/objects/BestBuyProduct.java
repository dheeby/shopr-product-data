package shopr.productdata.objects;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.sql.Date;

/**
 * Created by Neil on 9/25/2016.
 *
 * @author Neil Allison
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({"upc", "productId", "name", "type", "regularPrice", "salePrice", "onSale", "image", "thumbnailImage",
"shortDescription", "longDescription", "customerReviewCount", "customerReviewAverage", "ds", "pipelineName"})
public class BestBuyProduct
{
    private String upc;
    private Long productId;
    private String name;
    private String type;
    private Double regularPrice;
    private Double salePrice;
    private Boolean onSale;
    private String image;
    private String thumbnailImage;
    private String shortDescription;
    private String longDescription;
    private Long customerReviewCount;
    private String customerReviewAverage;
    private Date ds;
    private String pipelineName;

    public BestBuyProduct()
    {
        // Explicit default needed for Jackson
    }

    public String getUpc()
    {
        return upc;
    }

    public void setUpc(String sku)
    {
        this.upc = sku;
    }

    public Long getProductId()
    {
        return productId;
    }

    public void setProductId(Long productId)
    {
        this.productId = productId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
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

    public Boolean getOnSale()
    {
        return onSale;
    }

    public void setOnSale(Boolean onSale)
    {
        this.onSale = onSale;
    }

    public String getImage()
    {
        return image;
    }

    public void setImage(String image)
    {
        this.image = image;
    }

    public String getThumbnailImage()
    {
        return thumbnailImage;
    }

    public void setThumbnailImage(String thumbnailImage)
    {
        this.thumbnailImage = thumbnailImage;
    }

    public String getShortDescription()
    {
        return shortDescription;
    }

    public void setShortDescription(String shortDescription)
    {
        this.shortDescription = shortDescription;
    }

    public String getLongDescription()
    {
        return longDescription;
    }

    public void setLongDescription(String longDescription)
    {
        this.longDescription = longDescription;
    }

    public Long getCustomerReviewCount()
    {
        return customerReviewCount;
    }

    public void setCustomerReviewCount(Long customerReviewCount)
    {
        this.customerReviewCount = customerReviewCount;
    }

    public String getCustomerReviewAverage()
    {
        return customerReviewAverage;
    }

    public void setCustomerReviewAverage(String customerReviewAverage)
    {
        this.customerReviewAverage = customerReviewAverage;
    }

    public Date getDs()
    {
        return ds;
    }

    public void setDs(Date ds)
    {
        this.ds = ds;
    }

    public String getPipelineName()
    {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName)
    {
        this.pipelineName = pipelineName;
    }
}
