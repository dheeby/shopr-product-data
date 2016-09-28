package shopr.productdata.objects;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Neil on 9/26/2016.
 * @author Neil Allison
 */
@SuppressWarnings("unused")
public class WalMartTaxonomyTreeCategory
{
    private String id;
    private String name;
    private List<WalMartTaxonomyTreeCategory> children;

    public WalMartTaxonomyTreeCategory(String id, String name)
    {
        this.id = id;
        this.name = name;
        children = new ArrayList<>();
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<WalMartTaxonomyTreeCategory> getChildren()
    {
        return children;
    }

    public void setChildren(List<WalMartTaxonomyTreeCategory> children)
    {
        this.children = children;
    }
}
