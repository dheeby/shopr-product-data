package shopr.productdata.objects;

/**
 * Created by Neil on 9/25/2016.
 * @author Neil Allison
 */
@SuppressWarnings("unused")
public class WalMartProduct
{
    private Long upc;
    private Long itemId;
    private String name;
    private String category;
    private Double regularPrice;
    private Double salePrice;
    private Boolean onSale;
    private String thumbnailImage;
    private String largeImage;
    private String shortDescription;
    private String longDescription;
    private Long customerReviewCount;
    private String customerReviewAverage;

    public WalMartProduct()
    {
        // Explicit default needed for Jackson
    }

    public Long getUpc()
    {
        return upc;
    }

    public void setUpc(Long upc)
    {
        this.upc = upc;
    }

    public Long getItemId()
    {
        return itemId;
    }

    public void setItemId(Long itemId)
    {
        this.itemId = itemId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(String category)
    {
        this.category = category;
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

    public String getThumbnailImage()
    {
        return thumbnailImage;
    }

    public void setThumbnailImage(String thumbnailImage)
    {
        this.thumbnailImage = thumbnailImage;
    }

    public String getLargeImage()
    {
        return largeImage;
    }

    public void setLargeImage(String largeImage)
    {
        this.largeImage = largeImage;
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
}
