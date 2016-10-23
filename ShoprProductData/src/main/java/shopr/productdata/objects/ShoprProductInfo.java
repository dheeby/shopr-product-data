package shopr.productdata.objects;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Created by Neil on 10/22/2016.
 *
 * @author Neil Allison
 */
@SuppressWarnings("unused")
@JsonPropertyOrder({"upc", "name", "image", "thumbnail", "shortDescription", "longDescription",
        "customerReviewCount", "customerReviewAverage", "vendor", "categoryPath"})
public class ShoprProductInfo
{
    private String upc;
    private String name;
    private String image;
    private String thumbnail;
    private String shortDescription;
    private String longDescription;
    private Long customerReviewCount;
    private String customerReviewAverage;
    private String vendor;
    private String categoryPath;

    public ShoprProductInfo()
    {
        // Explicit default needed for Jackson
    }

    public String getUpc()
    {
        return upc;
    }

    public void setUpc(String upc)
    {
        this.upc = upc;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getImage()
    {
        return image;
    }

    public void setImage(String image)
    {
        this.image = image;
    }

    public String getThumbnail()
    {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail)
    {
        this.thumbnail = thumbnail;
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

    public String getVendor()
    {
        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    public String getCategoryPath()
    {
        return categoryPath;
    }

    public void setCategoryPath(String categoryPath)
    {
        this.categoryPath = categoryPath;
    }
}
